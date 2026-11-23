window.__ModuleLoader__.load({
  id: '@local/gemini-pool-panel',
  factory(require) {
    const React = require('react');
    const { useState, useEffect, useRef } = React;
    const h = React.createElement;
    const TAG = '[gemini-pool-panel]';
    console.log(TAG, 'module loaded');

    function fmtTime(ts) {
      const d = new Date((ts || 0) * 1000);
      const p = n => String(n).padStart(2, '0');
      return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
    }

    function PoolPanel() {
      const [data, setData] = useState(null);
      const [offline, setOffline] = useState(false);
      const [open, setOpen] = useState(false);
      const busy = useRef(false);

      useEffect(() => {
        const ctrl = new AbortController();
        let timer = null;
        const poll = async () => {
          if (busy.current) return;
          busy.current = true;
          try {
            const res = await fetch('http://127.0.0.1:8791/activity', {
              cache: 'no-store', signal: ctrl.signal,
            });
            if (!res.ok) throw new Error('status ' + res.status);
            setData(await res.json());
            setOffline(false);
          } catch (e) {
            if (!ctrl.signal.aborted) setOffline(true);
          } finally {
            busy.current = false;
          }
        };
        poll();
        timer = setInterval(poll, 2000);
        return () => { clearInterval(timer); ctrl.abort(); };
      }, []);

      const stats = (data && data.stats) || {};
      const accounts = (data && data.accounts) || [];
      const active = (data && data.active) || [];
      const recent = (data && data.recent) || [];
      const inflight = stats.inflight || 0;
      const cooling = accounts.some(a => a.state === 'cooling');

      const dot = offline ? '#f85149' : inflight ? '#3fb950' : cooling ? '#d29922' : '#8b949e';
      const summary = offline ? 'offline' : inflight ? inflight + ' in flight' : 'idle';

      const calls = (!offline && active.length)
        ? active.slice(0, 2).map((c, i) => h('span', {
            key: 'c' + i,
            style: { marginLeft: 8, opacity: .85, maxWidth: 150, overflow: 'hidden',
                     textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
          }, (c.model || '?') + ' \u00b7 ' + (c.step || '?') + ' \u00b7 ' +
             Math.round(c.elapsed_s || 0) + 's'))
        : null;

      const pop = open ? h('div', {
        onClick: e => e.stopPropagation(),
        style: {
          position: 'absolute', bottom: '100%', left: 0, marginBottom: 6,
          width: 420, maxWidth: '80vw', maxHeight: 300, overflowY: 'auto',
          background: 'Canvas', color: 'CanvasText', fontSize: 11,
          border: '1px solid rgba(127,127,127,.45)', borderRadius: 6,
          padding: 8, zIndex: 9999, cursor: 'default',
          boxShadow: '0 6px 18px rgba(0,0,0,.35)', textAlign: 'left',
        },
      },
        h('div', { style: { fontWeight: 600, marginBottom: 4 } }, 'Accounts'),
        accounts.length ? accounts.map((a, i) => h('div', {
          key: 'a' + i, style: { display: 'flex', gap: 8, lineHeight: 1.5 },
        },
          h('span', { style: { flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' } }, a.email),
          h('span', { style: { opacity: .7 } }, a.state),
          a.state === 'cooling'
            ? h('span', { style: { color: '#d29922' } }, Math.ceil(a.cooldown_s || 0) + 's')
            : null)) : h('div', { style: { opacity: .6 } }, 'no accounts yet'),
        h('div', { style: { fontWeight: 600, margin: '8px 0 4px' } }, 'Recent'),
        recent.length ? recent.slice(-5).reverse().map((r, i) => h('div', {
          key: 'r' + i, style: { display: 'flex', gap: 8, lineHeight: 1.5 },
        },
          h('span', { style: { opacity: .7 } }, fmtTime(r.at)),
          h('span', { style: { flex: 1, overflow: 'hidden', textOverflow: 'ellipsis',
                               whiteSpace: 'nowrap' } }, r.model),
          h('span', { style: { opacity: .7 } }, (r.ms || 0) + 'ms'),
          h('span', { style: { color: r.status === 'OK' ? '#3fb950' : '#d29922' } }, r.status)))
          : h('div', { style: { opacity: .6 } }, 'no calls yet')) : null;

      return h('div', {
        onClick: () => setOpen(v => !v),
        title: 'Gemini pool activity (click for details)',
        style: {
          position: 'relative', display: 'flex', alignItems: 'center',
          gap: 6, height: 26, width: '100%', boxSizing: 'border-box',
          padding: '0 8px', fontSize: 11, lineHeight: 1,
          color: 'inherit', font: 'inherit',
          background: 'rgba(127,127,127,.12)',
          border: '1px solid rgba(127,127,127,.35)', borderRadius: 6,
          cursor: 'pointer', userSelect: 'none', overflow: 'hidden',
        },
      },
        pop,
        h('span', { style: { width: 7, height: 7, borderRadius: '50%',
                             background: dot, flexShrink: 0 } }),
        h('span', { style: { fontWeight: 600 } }, 'Gemini pool'),
        h('span', { style: { opacity: offline ? .55 : 1 } }, summary),
        calls);
    }

    return {
      inject: ['slots'],
      apply(ctx) {
        console.log(TAG, 'apply');
        ctx.slots.inject('conversation.composer.dock', function* () {
          yield ctx.slots.register({
            name: 'conversation.composer.dock',
            id: 'gemini-pool-panel',
            order: 5,
          }, PoolPanel);
        });
      },
    };
  },
});
