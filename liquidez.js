/**
 * liquidez.js — Liquidaciones reales, posicionamiento y divergencia.
 * Módulo complementario de index.html. Sigue el activo seleccionado en la app.
 *
 * Fuentes (verificadas en vivo el 08/09/2026 desde navegador, sin bloqueos):
 *   [MEDIDO]  Binance Futures REST  → funding, open interest, top traders L/S, global L/S
 *   [MEDIDO]  Binance WS forceOrder → liquidaciones REALES en vivo (acumuladas en localStorage)
 *   [MEDIDO]  Bybit REST 4h         → divergencia del activo frente a BTC (7d y 30d)
 *   [ESTIMADO-MODELO] Zonas de liquidación proyectadas sobre swings — NIVEL 4, nunca deciden
 *
 * Integración: <script src="liquidez.js"></script> antes de </body>.
 *   · Lee window.APP_ASSET (lo publica index.html) para saber qué activo analizar.
 *   · Expone window.LIQ_DATA  → JSON con todo.
 *   · Expone window.LIQ_REPORT() → bloque de texto que index.html mete DENTRO del informe,
 *     que es lo único que acaba leyendo el analista IA.
 *   · Expone window.LIQ_SET_ASSET() → lo llama index.html al cambiar de moneda.
 */
(function () {
  'use strict';

  var FAPI = 'https://fapi.binance.com';
  var BYBIT = 'https://api.bybit.com';
  var LEVERAGES = [10, 25, 50, 100];
  var MMR = 0.005;
  var REFRESH_MS = 5 * 60 * 1000;

  window.LIQ_DATA = { estado: 'cargando', actualizado: null, activo: null };

  function activo() {
    var a = window.APP_ASSET;
    if (a && a.sym) return a;
    return { id: 'NEAR', name: 'NEAR', sym: 'NEARUSDT', prec: 4 };
  }
  function esBTC() { return activo().sym === 'BTCUSDT'; }

  function j(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status + ' en ' + url.split('?')[0].split('/').pop());
      return r.json();
    });
  }
  function fmtUsd(v) {
    if (!isFinite(v)) return 'n/d';
    if (v >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
    if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'K';
    return '$' + v.toFixed(0);
  }
  // Formatea un precio por su magnitud, no por la precisión del activo seleccionado:
  // así BTC no sale con cuatro decimales cuando la moneda activa es NEAR.
  function fpx(v) {
    v = +v;
    if (!isFinite(v)) return 'n/d';
    if (v >= 1000) return v.toFixed(0);
    if (v >= 10) return v.toFixed(2);
    if (v >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }
  function corr(x, y) {
    var n = Math.min(x.length, y.length), mx = 0, my = 0, i;
    if (!n) return 0;
    for (i = 0; i < n; i++) { mx += x[i]; my += y[i]; }
    mx /= n; my /= n;
    var num = 0, dx = 0, dy = 0;
    for (i = 0; i < n; i++) { num += (x[i] - mx) * (y[i] - my); dx += Math.pow(x[i] - mx, 2); dy += Math.pow(y[i] - my, 2); }
    return Math.sqrt(dx * dy) < 1e-12 ? 0 : num / Math.sqrt(dx * dy);
  }
  function logRet(c) { var o = [], i; for (i = 1; i < c.length; i++) o.push(Math.log(c[i] / c[i - 1])); return o; }

  function fetchPosicionamiento() {
    var S = activo().sym;
    return Promise.all([
      j(FAPI + '/fapi/v1/premiumIndex?symbol=' + S),
      j(FAPI + '/fapi/v1/premiumIndex?symbol=BTCUSDT'),
      j(FAPI + '/futures/data/openInterestHist?symbol=' + S + '&period=1h&limit=25'),
      j(FAPI + '/futures/data/topLongShortPositionRatio?symbol=' + S + '&period=1h&limit=1'),
      j(FAPI + '/futures/data/globalLongShortAccountRatio?symbol=' + S + '&period=1h&limit=1')
    ]).then(function (r) {
      var hist = r[2];
      var oiNow = parseFloat(hist[hist.length - 1].sumOpenInterestValue);
      var oi24h = parseFloat(hist[0].sumOpenInterestValue);
      return {
        etiqueta: 'MEDIDO', simbolo: S,
        fundingActivo: parseFloat(r[0].lastFundingRate),
        fundingBTC: parseFloat(r[1].lastFundingRate),
        precioActivo: parseFloat(r[0].markPrice),
        precioBTC: parseFloat(r[1].markPrice),
        oiUsd: oiNow,
        oiDelta24hPct: oi24h > 0 ? (oiNow / oi24h - 1) * 100 : NaN,
        topTradersLS: parseFloat(r[3][0].longShortRatio),
        globalLS: parseFloat(r[4][0].longShortRatio)
      };
    });
  }

  function lsKey() { return 'liq_events_' + activo().sym; }
  function loadEvents() { try { return JSON.parse(localStorage.getItem(lsKey())) || []; } catch (e) { return []; } }
  function saveEvents(ev) {
    var cut = Date.now() - 26 * 3600e3;
    ev = ev.filter(function (e) { return e.t > cut; }).slice(-4000);
    try { localStorage.setItem(lsKey(), JSON.stringify(ev)); } catch (e) {}
    return ev;
  }
  var events = [];
  function firstSeenKey() { return 'liq_first_seen_' + activo().sym; }
  function firstSeen() {
    var k = firstSeenKey(), v = localStorage.getItem(k);
    if (!v) { v = String(Date.now()); try { localStorage.setItem(k, v); } catch (e) {} }
    return +v;
  }

  var ws = null, wsCerradoAdrede = false;
  function startWS() {
    wsCerradoAdrede = false;
    var streams = activo().sym.toLowerCase() + '@forceOrder';
    if (!esBTC()) streams += '/btcusdt@forceOrder';
    try { ws = new WebSocket('wss://fstream.binance.com/stream?streams=' + streams); }
    catch (e) { return; }
    ws.onmessage = function (msg) {
      try {
        var o = JSON.parse(msg.data).data.o;
        events.push({
          t: Date.now(), s: o.s,
          lado: o.S === 'SELL' ? 'long' : 'short',
          usd: parseFloat(o.q) * parseFloat(o.ap),
          precio: parseFloat(o.ap)
        });
        events = saveEvents(events);
        render();
      } catch (e) {}
    };
    ws.onclose = function () { if (!wsCerradoAdrede) setTimeout(startWS, 5000); };
    ws.onerror = function () { try { ws.close(); } catch (e) {} };
  }
  function stopWS() { wsCerradoAdrede = true; try { if (ws) ws.close(); } catch (e) {} }

  function resumenLiq(sym) {
    var cut = Date.now() - 24 * 3600e3, L = 0, S = 0, n = 0, mayor = null;
    events.forEach(function (e) {
      if (e.s === sym && e.t > cut) {
        n++;
        if (e.lado === 'long') L += e.usd; else S += e.usd;
        if (!mayor || e.usd > mayor.usd) mayor = e;
      }
    });
    return { longs: L, shorts: S, eventos: n, mayor: mayor };
  }

  function fetchDivergencia() {
    if (esBTC()) return Promise.resolve(null);
    var S = activo().sym;
    return Promise.all([
      j(BYBIT + '/v5/market/kline?category=linear&symbol=' + S + '&interval=240&limit=180'),
      j(BYBIT + '/v5/market/kline?category=linear&symbol=BTCUSDT&interval=240&limit=180')
    ]).then(function (r) {
      var nc = r[0].result.list.map(function (k) { return +k[4]; }).reverse();
      var bc = r[1].result.list.map(function (k) { return +k[4]; }).reverse();
      var nr = logRet(nc), br = logRet(bc);
      var a = 0, b = 0, i;
      for (i = Math.max(0, nr.length - 6); i < nr.length; i++) { a += nr[i]; b += br[i]; }
      return {
        etiqueta: 'MEDIDO',
        corr30d: +corr(nr, br).toFixed(3),
        corr7d: +corr(nr.slice(-42), br.slice(-42)).toFixed(3),
        excesoHoyPts: +((a - b) * 100).toFixed(2),
        activoHoyPct: +(a * 100).toFixed(2),
        btcHoyPct: +(b * 100).toFixed(2)
      };
    });
  }

  function fetchZonas(sym, precioActual) {
    return j(BYBIT + '/v5/market/kline?category=linear&symbol=' + sym + '&interval=240&limit=42').then(function (r) {
      var ks = r.result.list.map(function (k) { return { h: +k[2], l: +k[3], vol: +k[5] }; }).reverse();
      var niveles = [];
      ks.forEach(function (k, idx) {
        var esMax = (idx === 0 || k.h >= ks[idx - 1].h) && (idx === ks.length - 1 || k.h >= ks[idx + 1].h);
        var esMin = (idx === 0 || k.l <= ks[idx - 1].l) && (idx === ks.length - 1 || k.l <= ks[idx + 1].l);
        LEVERAGES.forEach(function (lev) {
          var f = 1 / lev - MMR;
          if (esMax) niveles.push({ p: k.h * (1 + f), w: k.vol / lev });
          if (esMin) niveles.push({ p: k.l * (1 - f), w: k.vol / lev });
        });
      });
      if (!niveles.length) return { etiqueta: 'ESTIMADO-MODELO', arriba: [], abajo: [] };
      var banda = precioActual * 0.004, bandas = {};
      niveles.forEach(function (n) { var k = Math.round(n.p / banda); bandas[k] = (bandas[k] || 0) + n.w; });
      var lista = Object.keys(bandas).map(function (k) { return { precio: +(k * banda), peso: bandas[k] }; });
      var maxW = Math.max.apply(null, lista.map(function (x) { return x.peso; }));
      lista.forEach(function (x) { x.densidad = +(x.peso / maxW).toFixed(2); delete x.peso; });
      var arriba = lista.filter(function (x) { return x.precio > precioActual && x.densidad > 0.3; })
                        .sort(function (a, b) { return b.densidad - a.densidad; }).slice(0, 3);
      var abajo = lista.filter(function (x) { return x.precio < precioActual && x.densidad > 0.3; })
                       .sort(function (a, b) { return b.densidad - a.densidad; }).slice(0, 3);
      return { etiqueta: 'ESTIMADO-MODELO', arriba: arriba, abajo: abajo };
    });
  }

  window.LIQ_REPORT = function () {
    var d = window.LIQ_DATA, A = activo();
    if (d.estado === 'cargando') return 'LIQUIDACIONES Y POSICIONAMIENTO: todavía cargando desde Binance. Vuelve a generar el informe en unos segundos.\n';
    if (String(d.estado).indexOf('error') === 0) return 'LIQUIDACIONES Y POSICIONAMIENTO: DATO NO DISPONIBLE (' + d.estado + ')\n';

    var liqA = resumenLiq(A.sym), liqB = resumenLiq('BTCUSDT');
    var horas = (Date.now() - firstSeen()) / 3600e3;
    var t = '';

    t += 'LIQUIDACIONES REALES EN VIVO [MEDIDO · Binance WebSocket]  ← NIVEL 2, flujo real\n';
    t += '  Ventana observada: ' + (horas < 1 ? Math.round(horas * 60) + ' minutos' : horas.toFixed(1) + ' horas') + ' (solo acumula con la app abierta)\n';
    if (horas < 2) t += '  ⚠ Ventana corta: no saques conclusiones de la ausencia de liquidaciones.\n';
    t += '  ' + A.name + ': longs liquidados ' + fmtUsd(liqA.longs) + ' | shorts liquidados ' + fmtUsd(liqA.shorts) + ' (' + liqA.eventos + ' eventos)\n';
    if (liqA.mayor) t += '     Mayor evento: ' + fmtUsd(liqA.mayor.usd) + ' de ' + liqA.mayor.lado + 's a $' + fpx(liqA.mayor.precio) + '\n';
    if (!esBTC()) t += '  BTC: longs ' + fmtUsd(liqB.longs) + ' | shorts ' + fmtUsd(liqB.shorts) + ' (' + liqB.eventos + ' eventos)\n';
    if (liqA.eventos > 0) {
      t += '  → ' + (liqA.longs > liqA.shorts * 1.5 ? 'Dominan liquidaciones de LONGS: el dolor está en los compradores apalancados.'
            : liqA.shorts > liqA.longs * 1.5 ? 'Dominan liquidaciones de SHORTS: posible squeeze al alza en curso.'
            : 'Liquidaciones repartidas entre ambos lados: sin sesgo claro.') + '\n';
    }

    var P = d.posicionamiento;
    if (P) {
      t += '\nPOSICIONAMIENTO [MEDIDO · Binance Futures]  ← NIVEL 2\n';
      t += '  Funding ' + A.name + ': ' + (P.fundingActivo * 100).toFixed(4) + '%  ·  Funding BTC: ' + (P.fundingBTC * 100).toFixed(4) + '%\n';
      t += '  Open Interest: ' + fmtUsd(P.oiUsd) + (isFinite(P.oiDelta24hPct) ? '  (' + (P.oiDelta24hPct >= 0 ? '+' : '') + P.oiDelta24hPct.toFixed(1) + '% en 24h)' : '') + '\n';
      t += '  Ratio long/short de TOP TRADERS (por posición): ' + P.topTradersLS.toFixed(2) + '\n';
      t += '  Ratio long/short GLOBAL (por cuentas):          ' + P.globalLS.toFixed(2) + '\n';
      var brecha = P.topTradersLS - P.globalLS;
      t += '  → ' + (brecha > 0.4 ? 'Las ballenas están MÁS largas que el minorista: el dinero grande acompaña la subida.'
             : brecha < -0.4 ? 'Las ballenas están MENOS largas que el minorista: el minorista va solo. Señal de aviso.'
             : 'Ballenas y minorista posicionados de forma parecida: sin divergencia de posicionamiento.') + '\n';
      if (P.topTradersLS > 2.5 || P.globalLS > 2.5) t += '  ⚠ Posicionamiento muy cargado al alza: combustible para un flush a la baja.\n';
      if (P.topTradersLS < 0.7 || P.globalLS < 0.7) t += '  ⚠ Posicionamiento muy cargado a la baja: combustible para un squeeze al alza.\n';
    }

    var V = d.divergencia;
    if (V) {
      t += '\nDIVERGENCIA ' + A.name + '/BTC [MEDIDO · Bybit velas de 4h]  ← NIVEL 3, matiza\n';
      t += '  Correlación 7 días: ' + V.corr7d + '   ·   30 días: ' + V.corr30d + '\n';
      t += '  Último día: ' + A.name + ' ' + V.activoHoyPct + '% vs BTC ' + V.btcHoyPct + '% → exceso ' + V.excesoHoyPts + ' puntos\n';
      if (Math.abs(V.excesoHoyPts) > 2) t += '  ⚠ Más de 2 puntos de exceso: revisa funding y lado de las liquidaciones antes de atribuirlo a fuerza propia.\n';
      if (V.corr7d < 0.4) t += '  🚨 Correlación por debajo de 0,4: el movimiento es idiosincrático, BTC no lo explica.\n';
    }

    function zonasTxt(z, etiqueta) {
      if (!z) return '  ' + etiqueta + ' DATO NO DISPONIBLE\n';
      var f = function (arr, flecha) {
        return arr.length ? arr.map(function (x) { return flecha + ' $' + fpx(x.precio) + ' (densidad ' + x.densidad + ')'; }).join('  ·  ')
                          : flecha + ' sin clúster relevante';
      };
      return '  ' + etiqueta + '\n     ' + f(z.arriba, '▲') + '\n     ' + f(z.abajo, '▼') + '\n';
    }
    t += '\nZONAS DE LIQUIDACIÓN PROYECTADAS [ESTIMADO-MODELO]  ← NIVEL 4, NUNCA deciden\n';
    t += '  Método: swings de 4h de los últimos 7 días, apalancamientos 10x a 100x, ponderado por volumen.\n';
    t += '  NO es un heatmap con open interest real. No lo cites como confluencia.\n';
    t += zonasTxt(d.zonasActivo, A.name + ':');
    if (!esBTC()) t += zonasTxt(d.zonasBTC, 'BTC:');
    t += '  Un clúster es un IMÁN, no un suelo: el precio va hacia la liquidez y suele atravesarla.\n';
    t += '  Sirven como candidatos a objetivo y a barrido, nunca como soporte ni como invalidación.\n';
    return t;
  };

  var panel;
  function ensurePanel() {
    if (panel) return panel;
    var host = document.getElementById('liquidez') || document.body;
    panel = document.createElement('section');
    panel.id = 'liq-panel';
    panel.style.cssText = 'font-family:-apple-system,system-ui,sans-serif;background:#13161e;color:#e8e9ec;border:.5px solid rgba(255,107,107,.25);border-radius:10px;padding:12px;margin:12px auto;font-size:12px;line-height:1.6;max-width:680px';
    host.appendChild(panel);
    return panel;
  }
  function render() {
    var d = window.LIQ_DATA, A = activo();
    var p = ensurePanel();
    var liqA = resumenLiq(A.sym);
    var horas = (Date.now() - firstSeen()) / 3600e3;
    d.activo = A.sym;
    d.liquidacionesVivo = { etiqueta: 'MEDIDO', ventanaHoras: +horas.toFixed(2), activo: liqA, BTC: resumenLiq('BTCUSDT') };
    d.actualizado = new Date().toISOString();
    var P = d.posicionamiento, V = d.divergencia;
    p.innerHTML =
      '<div style="font-size:11px;font-weight:700;color:#ff6b6b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">🔥 Liquidez y posicionamiento · ' + A.name + '</div>' +
      'Liquidaciones reales (' + (horas < 1 ? Math.round(horas * 60) + ' min' : horas.toFixed(1) + ' h') + ' observadas): longs ' +
      fmtUsd(liqA.longs) + ' · shorts ' + fmtUsd(liqA.shorts) + ' · ' + liqA.eventos + ' eventos<br>' +
      (P ? ('Funding ' + (P.fundingActivo * 100).toFixed(4) + '% · OI ' + fmtUsd(P.oiUsd) +
            ' (' + (P.oiDelta24hPct >= 0 ? '+' : '') + P.oiDelta24hPct.toFixed(1) + '% 24h) · Top L/S ' +
            P.topTradersLS.toFixed(2) + ' vs global ' + P.globalLS.toFixed(2)) : 'Posicionamiento: cargando…') + '<br>' +
      (V ? ('Correlación con BTC 7d ' + V.corr7d + ' · exceso hoy ' + V.excesoHoyPts + ' pts') : (esBTC() ? '' : 'Divergencia: cargando…')) +
      '<div style="margin-top:8px;color:#6b7080;font-size:11px">Estos datos ya van dentro del informe que copias. Estado: ' + d.estado + '</div>';
  }

  function refresh() {
    var A = activo();
    Promise.all([fetchPosicionamiento(), fetchDivergencia()])
      .then(function (r) {
        window.LIQ_DATA.posicionamiento = r[0];
        window.LIQ_DATA.divergencia = r[1];
        return Promise.all([
          fetchZonas(A.sym, r[0].precioActivo),
          esBTC() ? Promise.resolve(null) : fetchZonas('BTCUSDT', r[0].precioBTC)
        ]);
      })
      .then(function (z) {
        window.LIQ_DATA.zonasActivo = z[0];
        window.LIQ_DATA.zonasBTC = z[1];
        window.LIQ_DATA.estado = 'ok';
        render();
      })
      .catch(function (e) {
        window.LIQ_DATA.estado = 'error: ' + e.message;
        render();
      });
  }

  window.LIQ_SET_ASSET = function () {
    stopWS();
    window.LIQ_DATA = { estado: 'cargando', actualizado: null, activo: activo().sym };
    events = loadEvents();
    firstSeen();
    startWS();
    render();
    refresh();
  };

  function init() {
    events = loadEvents();
    firstSeen();
    startWS();
    render();
    refresh();
    setInterval(refresh, REFRESH_MS);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
