window.__ModuleLoader__.load({
  id: '@local/gemini-pool-panel',
  factory(require) {
    const React = require('react');
    const { useState, useEffect, useRef } = React;
    const h = React.createElement;

    function formatTime(ts) {
      const d = new Date(ts * 1000);
      return [d.getHours(), d.getMinutes(), d.getSeconds()]
        .map(n => String(n).padStart(2, '0')).join(':');
    }

    function PoolPanel() {
      const [data, setData] = useState(null);
      const [offline, setOffline] = useState(false);
      const [open, setOpen] = useState(false);
      const busyRef = useRef(false);

      useEffect(() => {
        const abortCtrl = new AbortController();
        let timer = null;

        async function poll() {
          if (busyRef.current) return;
          busyRef.current = true;
          try {
            const res = await fetch('http://127.0.0.1:8791/activity', {
              cache: 'no-store',
              signal: abortCtrl.signal,
            });
            if (!res.ok) throw new Error('bad status');
            setData(await res.json());
            setOffline(false);
          } catch {
            if (!abortCtrl.signal.aborted) setOffline(true);
          } finally {
            busyRef.current = false;
          }
        }

        poll();
        timer = setInterval(poll, 2000);
        return () => {
          clearInterval(timer);
          abortCtrl.abort();
        };
      }, []);

      const stats = data?.stats;
      const accounts = data?.accounts || [];
      const active = data?.active || [];
      const recent = data?.recent || [];

      let dotColor = '#8b949e';
      if (!offline && stats) {
        if (stats.inflight > 0) dotColor = '#3fb950';
        else if (accounts.some(a => a.state === 'cooling')) dotColor = '#d29922';
      }

      const summary = offline
        ? 'offline'
        : (stats && stats.inflight > 0 ? `${stats.inflight} in flight` : 'idle');

      const activeNodes = !offline && active.length
        ? active.slice(0, 2).map((c, i) => h('span', {
            key: c.id || i,
            style: {
              marginLeft: 6, maxWidth: 160, whiteSpace: 'nowrap',
              overflow: 'hidden', textOverflow: 'ellipsis', opacity: 0.85,
            },
          }, `${c.model} · ${c.step} · ${Math.round(c.elapsed_s || 0)}s`))
        : null;

      const popover = open ? h('div', {
        onClick: e => e.stopPropagation(),
        style: {
          position: 'absolute', bottom: '100%', left: 0, marginBottom: 4,
          width: 420, maxWidth: '80vw', maxHeight: 320, overflowY: 'auto',
          background: 'Canvas', color: 'CanvasText',
          border: '1px solid rgba(127,127,127,.35)', borderRadius: 6,
          padding: '8px', zIndex: 1000, fontSize: 11, cursor: 'default',
          boxShadow: '0 4px 12px rgba(0,0,0,.25)',
        },
      },
        h('div', { style: { fontWeight: 600, marginBottom: 4 } }, 'Accounts'),
        accounts.map((a, i) => h('div', {
          key: 'acc' + i,
          style: { display: 'flex', gap: 6, lineHeight: 1.5 },
        },
          h('span', { style: { flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' } }, a.email),
          h('span', { style: { opacity: 0.7 } }, a.state),
          a.state === 'cooling'
            ? h('span', { style: { color: '#d29922' } }, `${Math.ceil(a.cooldown_s || 0)}s`)
            : null)),
        h('div', { style: { fontWeight: 600, marginTop: 8, marginBottom: 4 } }, 'Recent calls'),
        recent.length === 0
          ? h('div', { style: { opacity: 0.6 } }, 'no calls yet')
          : recent.slice(-5).reverse().map((r, i) => h('div', {
              key: 'rec' + i,
              style: { display: 'flex', gap: 6, lineHeight: 1.5 },
            },
              h('span', { style: { opacity: 0.7 } }, formatTime(r.at)),
              h('span', {
                style: {
                  flex: 1, overflow: 'hidden', textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                },
              }, r.model),
              h('span', { style: { opacity: 0.7 } }, `${r.ms}ms`),
              h('span', {
                style: { color: r.status === 'OK' ? '#3fb950' : '#d29922' },
              }, r.status))))
        : null;

      return h('div', {
        onClick: () => setOpen(!open),
        title: 'Gemini pool activity (click for details)',
        style: {
          position: 'relative', display: 'flex', alignItems: 'center',
          height: 28, width: '100%', boxSizing: 'border-box',
          background: 'transparent', color: 'inherit', font: 'inherit',
          fontSize: 11, border: '1px solid rgba(127,127,127,.35)',
          borderRadius: 6, padding: '2px 8px', cursor: 'pointer',
          userSelect: 'none', overflow: 'hidden',
        },
      },
        popover,
        h('span', {
          style: {
            width: 7, height: 7, borderRadius: '50%',
            backgroundColor: dotColor, marginRight: 6, flexShrink: 0,
          },
        }),
        h('span', { style: { fontWeight: 600, marginRight: 6 } }, 'Gemini pool'),
        h('span', { style: { opacity: offline ? 0.6 : 1 } }, summary),
        activeNodes);
    }

    return {
      inject: ['slots'],
      apply(ctx) {
        ctx.slots.inject('conversation.composer.dock', () => ctx.slots.register({
          name: 'conversation.composer.dock',
          id: 'gemini-pool-panel',
          order: 5,
        }, PoolPanel));
      },
    };
  },
});
