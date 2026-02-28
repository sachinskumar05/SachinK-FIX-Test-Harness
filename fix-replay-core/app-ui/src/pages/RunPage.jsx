import { useAppState } from "../state/AppStateContext";

function Toggle({ label, checked, onChange, hint }) {
  return (
    <label className="toggle">
      <input type="checkbox" checked={checked} onChange={event => onChange(event.target.checked)} />
      <span>{label}</span>
      {hint ? <small className="field-hint">{hint}</small> : null}
    </label>
  );
}

export default function RunPage() {
  const {
    inputs,
    loading,
    setInput,
    runScan,
    runPrepare,
    runOffline,
    runOnline
  } = useAppState();

  const ignoreHandledError = promise => {
    promise.catch(() => {});
  };

  return (
    <section className="panel">
      <h2>Run Jobs</h2>
      <p className="field-hint">
        These controls call `app-server` only; all FIX processing remains in backend modules.
      </p>

      <div className="run-grid">
        <article className="run-card">
          <h3>Scan</h3>
          <Toggle
            label="Async"
            checked={inputs.scanAsync}
            onChange={value => setInput("scanAsync", value)}
          />
          <button type="button" onClick={() => ignoreHandledError(runScan())} disabled={loading}>
            Run /api/scan
          </button>
        </article>

        <article className="run-card">
          <h3>Prepare</h3>
          <Toggle
            label="Async"
            checked={inputs.prepareAsync}
            onChange={value => setInput("prepareAsync", value)}
          />
          <button type="button" onClick={() => ignoreHandledError(runPrepare())} disabled={loading}>
            Run /api/prepare
          </button>
        </article>

        <article className="run-card">
          <h3>Run Offline</h3>
          <Toggle
            label="Async"
            checked={inputs.offlineAsync}
            onChange={value => setInput("offlineAsync", value)}
          />
          <button type="button" onClick={() => ignoreHandledError(runOffline())} disabled={loading}>
            Run /api/run-offline
          </button>
        </article>

        <article className="run-card">
          <h3>Run Online</h3>
          <label className="field">
            <span className="field-label">Transport Class</span>
            <input
              type="text"
              value={inputs.transportClass}
              onChange={event => setInput("transportClass", event.target.value)}
            />
          </label>

          <label className="field">
            <span className="field-label">Transport Config Path (optional)</span>
            <input
              type="text"
              value={inputs.transportConfigPath}
              onChange={event => setInput("transportConfigPath", event.target.value)}
            />
          </label>

          <div className="grid-two">
            <label className="field">
              <span className="field-label">Receive Timeout (ms)</span>
              <input
                type="number"
                min="1"
                value={inputs.receiveTimeoutMs}
                onChange={event => setInput("receiveTimeoutMs", event.target.value)}
              />
            </label>
            <label className="field">
              <span className="field-label">Queue Capacity</span>
              <input
                type="number"
                min="1"
                value={inputs.queueCapacity}
                onChange={event => setInput("queueCapacity", event.target.value)}
              />
            </label>
          </div>

          <label className="field">
            <span className="field-label">Transport Properties JSON (optional)</span>
            <textarea
              rows={6}
              value={inputs.transportPropertiesText}
              onChange={event => setInput("transportPropertiesText", event.target.value)}
              placeholder='{"host":"127.0.0.1","port":"9999"}'
            />
          </label>

          <Toggle
            label="Async"
            checked={inputs.onlineAsync}
            onChange={value => setInput("onlineAsync", value)}
            hint="Defaults true on backend; keep enabled for long runs."
          />

          <button type="button" onClick={() => ignoreHandledError(runOnline())} disabled={loading}>
            Run /api/run-online
          </button>
        </article>
      </div>
    </section>
  );
}
