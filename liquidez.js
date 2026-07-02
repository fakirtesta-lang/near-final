/**
 * liquidez.js — Módulo de liquidaciones y divergencia NEAR/BTC para near-final
 * Fuentes 100% gratuitas y verificadas con CORS abierto desde GitHub Pages (02/07/2026):
 *   [MEDIDO]  Binance Futures REST  → funding, open interest, ratios long/short
 *   [MEDIDO]  Binance WS forceOrder → liquidaciones reales en vivo (acumuladas en localStorage)
 *   [MEDIDO]  Bybit REST            → klines 4h para el detector de divergencia
 *   [ESTIMADO-MODELO] Zonas de liquidación calculadas por apalancamientos estándar sobre swings
 *
 * Integración: <script src="liquidez.js"></script> antes de </body>.
 * Crea la sección #liq-panel al final de <body> (o dentro de #liquidez si existe).
 * Expone window.LIQ_DATA (JSON) para que el agente IA lo lea con una sola llamada JS.
 * Sin botones: todo se auto-refresca (REST cada 5 min, WS continuo). No congela la pestaña.
 */
(function () {
  'use strict';

  var FAPI = 'https://fapi.binance.com';
  var BYBIT = 'https://api.bybit.com';
  var LS_KEY = 'liq_events_v1';
  var LEVERAGES = [10, 25, 50, 100];        // apalancamientos estándar para zonas estimadas
  var MMR = 0.005;                          // margen de mantenimiento aprox.

  window.LIQ_DATA = { estado: 'cargando', actualizado: null };

  /* ---------- utilidades ---------- */
  function j(url) { return fetch(url).then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + url); return r.json(); }); }
  function fmtUsd(v) {
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
    if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'K';
    return '$' + v.toFixed(0);
  }
  function corr(x, y) {
    var n = Math.min(x.length, y.length), mx = 0, my = 0, i;
    for (i = 0; i < n; i++) { mx += x[i]; my += y[i]; }
    mx /= n; my /= n;
    var num = 0, dx = 0, dy = 0;
    for (i = 0; i < n; i++) { num += (x[i] - mx) * (y[i] - my); dx += Math.pow(x[i] - mx, 2); dy += Math.pow(y[i] - my, 2); }
    return num / Math.sqrt(dx * dy);
  }
  function logRet(closes) { var out = [], i; for (i = 1; i < closes.length; i++) out.push(Math.log(closes[i] / closes[i - 1])); return out; }

  /* ---------- 1. Posicionamiento [MEDIDO] (Binance fapi) ---------- */
  function fetchPosicionamiento() {
    return Promise.all([
      j(FAPI + '/fapi/v1/premiumIndex?symbol=NEARUSDT'),
      j(FAPI + '/fapi/v1/premiumIndex?symbol=BTCUSDT'),
      j(FAPI + '/futures/data/openInterestHist?symbol=NEARUSDT&period=1h&limit=25'),
      j(FAPI + '/futures/data/topLongShortPositionRatio?symbol=NEARUSDT&period=1h&limit=1'),
      j(FAPI + '/futures/data/globalLongShortAccountRatio?symbol=NEARUSDT&period=1h&limit=1')
    ]).then(function (r) {
      var oiNow = parseFloat(r[2][r[2].length - 1].sumOpenInterestValue);
      var oi24h = parseFloat(r[2][0].sumOpenInterestValue);
      return {
        etiqueta: 'MEDIDO',
        fundingNEAR: parseFloat(r[0].lastFundingRate),
        fundingBTC: parseFloat(r[1].lastFundingRate),
        precioNEAR: parseFloat(r[0].markPrice),
        precioBTC: parseFloat(r[1].markPrice),
        oiUsd: oiNow,
        oiDelta24hPct: (oiNow / oi24h - 1) * 100,
        topTradersLS: parseFloat(r[3][0].longShortRatio),
        globalLS: parseFloat(r[4][0].longShortRatio)
      };
    });
  }

  /* ---------- 2. Liquidaciones en vivo [MEDIDO] (WS forceOrder + localStorage) ---------- */
  function loadEvents() { try { return JSON.parse(localStorage.getItem(LS_KEY)) || []; } catch (e) { return []; } }
  function saveEvents(ev) {
    var cut = Date.now() - 26 * 3600e3;                 // conservar 26 h
    ev = ev.filter(function (e) { return e.t > cut; }).slice(-4000);
    try { localStorage.setItem(LS_KEY, JSON.stringify(ev)); } catch (e) { /* lleno: ignorar */ }
    return ev;
  }
  var events = loadEvents();
  var wsFirstSeen = localStorage.getItem('liq_first_seen') || String(Date.now());
  localStorage.setItem('liq_first_seen', wsFirstSeen);

  function startWS() {
    var ws;
    function connect() {
      ws = new WebSocket('wss://fstream.binance.com/stream?streams=nearusdt@forceOrder/btcusdt@forceOrder');
      ws.onmessage = function (msg) {
        try {
          var o = JSON.parse(msg.data).data.o;
          // SELL forzado = liquidación de LONG; BUY forzado = liquidación de SHORT
          events.push({ t: Date.now(), s: o.s, lado: o.S === 'SELL' ? 'long' : 'short', usd: parseFloat(o.q) * parseFloat(o.ap), precio: parseFloat(o.ap) });
          events = saveEvents(events);
          render();
        } catch (e) { /* ignorar */ }
      };
      ws.onclose = function () { setTimeout(connect, 5000); };  // reconexión
      ws.onerror = function () { ws.close(); };
    }
    connect();
  }
  function resumenLiq(sym) {
    var cut = Date.now() - 24 * 3600e3, L = 0, S = 0, n = 0;
    events.forEach(function (e) { if (e.s === sym && e.t > cut) { n++; if (e.lado === 'long') L += e.usd; else S += e.usd; } });
    return { longs: L, shorts: S, eventos: n };
  }

  /* ---------- 3. Divergencia NEAR/BTC [MEDIDO] (Bybit klines 4h) ---------- */
  function fetchDivergencia() {
    return Promise.all([
      j(BYBIT + '/v5/market/kline?category=linear&symbol=NEARUSDT&interval=240&limit=180'),
      j(BYBIT + '/v5/market/kline?category=linear&symbol=BTCUSDT&interval=240&limit=180')
    ]).then(function (r) {
      var nc = r[0].result.list.map(function (k) { return +k[4]; }).reverse();
      var bc = r[1].result.list.map(function (k) { return +k[4]; }).reverse();
      var nr = logRet(nc), br = logRet(bc);
      var last6n = 0, last6b = 0, i;
      for (i = nr.length - 6; i < nr.length; i++) { last6n += nr[i]; last6b += br[i]; }
      return {
        etiqueta: 'MEDIDO',
        corr30d: +corr(nr, br).toFixed(3),
        corr7d: +corr(nr.slice(-42), br.slice(-42)).toFixed(3),
        excesoHoyPts: +((last6n - last6b) * 100).toFixed(2),
        nearHoyPct: +(last6n * 100).toFixed(2),
        btcHoyPct: +(last6b * 100).toFixed(2)
      };
    });
  }

  /* ---------- 4. Zonas de liquidación [ESTIMADO-MODELO] ---------- */
  /* Método (equivalente a los indicadores comunitarios de TradingView):
     sobre las klines 4h de 7 días se detectan swings; se asume que en cada swing
     se abrieron posiciones con apalancamientos estándar y se proyectan sus
     precios de liquidación, ponderados por volumen. NO es el heatmap real. */
  function fetchZonas(sym, precioActual) {
    return j(BYBIT + '/v5/market/kline?category=linear&symbol=' + sym + '&interval=240&limit=42').then(function (r) {
      var ks = r.result.list.map(function (k) { return { h: +k[2], l: +k[3], vol: +k[5] }; }).reverse();
      var niveles = [];
      ks.forEach(function (k, idx) {
        var esMaxLocal = (idx === 0 || k.h >= ks[idx - 1].h) && (idx === ks.length - 1 || k.h >= ks[idx + 1].h);
        var esMinLocal = (idx === 0 || k.l <= ks[idx - 1].l) && (idx === ks.length - 1 || k.l <= ks[idx + 1].l);
        LEVERAGES.forEach(function (lev) {
          var f = 1 / lev - MMR;
          if (esMaxLocal) niveles.push({ p: k.h * (1 + f), w: k.vol / lev }); // shorts abiertos en máximos → liq arriba
          if (esMinLocal) niveles.push({ p: k.l * (1 - f), w: k.vol / lev }); // longs abiertos en mínimos → liq abajo
        });
      });
      // clúster en bandas del 0,4 %
      var bandas = {};
      niveles.forEach(function (n) {
        var key = Math.round(n.p / (precioActual * 0.004));
        bandas[key] = (bandas[key] || 0) + n.w;
      });
      var lista = Object.keys(bandas).map(function (k) { return { precio: +(k * precioActual * 0.004).toFixed(precioActual > 100 ? 0 : 4), peso: bandas[k] }; });
      var maxW = Math.max.apply(null, lista.map(function (x) { return x.peso; }));
      lista.forEach(function (x) { x.densidad = +(x.peso / maxW).toFixed(2); delete x.peso; });
      var arriba = lista.filter(function (x) { return x.precio > precioActual && x.densidad > 0.3; }).sort(function (a, b) { return b.densidad - a.densidad; }).slice(0, 3);
      var abajo = lista.filter(function (x) { return x.precio < precioActual && x.densidad > 0.3; }).sort(function (a, b) { return b.densidad - a.densidad; }).slice(0, 3);
      return { etiqueta: 'ESTIMADO-MODELO', arriba: arriba, abajo: abajo };
    });
  }

  /* ---------- render ---------- */
  var panel;
  function ensurePanel() {
    if (panel) return panel;
    var host = document.getElementById('liquidez') || document.body;
    panel = document.createElement('section');
    panel.id = 'liq-panel';
    panel.style.cssText = 'font-family:system-ui,sans-serif;background:#0d1117;color:#e6edf3;border:1px solid #30363d;border-radius:10px;padding:16px;margin:16px 0;font-size:14px;line-height:1.5';
    host.appendChild(panel);
    return panel;
  }
  function zonasHtml(z) {
    if (!z) return '—';
    var f = function (arr, flecha) {
      return arr.length ? arr.map(function (x) { return flecha + ' ' + x.precio + ' (dens. ' + x.densidad + ')'; }).join(' · ') : flecha + ' sin clúster relevante';
    };
    return f(z.arriba, '▲') + '<br>' + f(z.abajo, '▼');
  }
  function render() {
    var d = window.LIQ_DATA;
    var p = ensurePanel();
    var liqN = resumenLiq('NEARUSDT'), liqB = resumenLiq('BTCUSDT');
    var desde = new Date(+wsFirstSeen).toLocaleString('es-ES', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit' });
    d.liquidacionesVivo = { etiqueta: 'MEDIDO desde ' + desde, NEAR: liqN, BTC: liqB };
    d.actualizado = new Date().toISOString();
    p.innerHTML =
      '<h3 style="margin:0 0 10px;color:#58a6ff">🔥 Liquidez y divergencia <small style="color:#8b949e;font-weight:normal">' + new Date().toLocaleTimeString('es-ES') + '</small></h3>' +
      '<b>Liquidaciones en vivo</b> <small>[MEDIDO desde ' + desde + ' — acumula mientras la app esté abierta]</small><br>' +
      'NEAR: longs ' + fmtUsd(liqN.longs) + ' | shorts ' + fmtUsd(liqN.shorts) + ' (' + liqN.eventos + ' eventos) &nbsp;·&nbsp; ' +
      'BTC: longs ' + fmtUsd(liqB.longs) + ' | shorts ' + fmtUsd(liqB.shorts) + '<br><br>' +
      '<b>Posicionamiento</b> <small>[MEDIDO — Binance Futures]</small><br>' +
      (d.posicionamiento ? ('Funding NEAR ' + (d.posicionamiento.fundingNEAR * 100).toFixed(4) + '% vs BTC ' + (d.posicionamiento.fundingBTC * 100).toFixed(4) +
        '% · OI NEAR ' + fmtUsd(d.posicionamiento.oiUsd) + ' (' + d.posicionamiento.oiDelta24hPct.toFixed(1) + '% 24h) · Top traders L/S ' +
        d.posicionamiento.topTradersLS.toFixed(2) + ' · Global L/S ' + d.posicionamiento.globalLS.toFixed(2)) : 'cargando…') + '<br><br>' +
      '<b>Divergencia NEAR/BTC</b> <small>[MEDIDO — Bybit 4h]</small><br>' +
      (d.divergencia ? ('Corr 7d ' + d.divergencia.corr7d + ' (30d ' + d.divergencia.corr30d + ') · Hoy: NEAR ' + d.divergencia.nearHoyPct +
        '% vs BTC ' + d.divergencia.btcHoyPct + '% → exceso ' + d.divergencia.excesoHoyPts + ' pts' +
        (Math.abs(d.divergencia.excesoHoyPts) > 2 ? ' <span style="color:#f0883e">⚠️ &gt;2 pts: revisar funding y lado de liquidaciones</span>' : '') +
        (d.divergencia.corr7d < 0.4 ? ' <span style="color:#f85149">🚨 corr7d &lt; 0.4: movimiento idiosincrático</span>' : '')) : 'cargando…') + '<br><br>' +
      '<b>Zonas de liquidación estimadas</b> <small>[ESTIMADO-MODELO — apalancamientos 10-100x sobre swings 7d; NO es el heatmap real]</small><br>' +
      'NEAR:<br>' + zonasHtml(d.zonasNEAR) + '<br>BTC:<br>' + zonasHtml(d.zonasBTC) +
      '<div style="margin-top:10px;color:#8b949e;font-size:12px">Imanes = candidatos a TP y barridos; nunca soportes estructurales ni base de invalidación. window.LIQ_DATA expone todo en JSON para el agente.</div>';
  }

  /* ---------- ciclo de actualización ---------- */
  function refresh() {
    Promise.all([fetchPosicionamiento(), fetchDivergencia()])
      .then(function (r) {
        window.LIQ_DATA.posicionamiento = r[0];
        window.LIQ_DATA.divergencia = r[1];
        return Promise.all([fetchZonas('NEARUSDT', r[0].precioNEAR), fetchZonas('BTCUSDT', r[0].precioBTC)]);
      })
      .then(function (z) {
        window.LIQ_DATA.zonasNEAR = z[0];
        window.LIQ_DATA.zonasBTC = z[1];
        window.LIQ_DATA.estado = 'ok';
        render();
      })
      .catch(function (e) {
        window.LIQ_DATA.estado = 'error: ' + e.message;
        render();
      });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
  function init() {
    startWS();
    refresh();
    setInterval(refresh, 5 * 60 * 1000);   // REST cada 5 min (dentro de los rate limits públicos)
  }
})();
