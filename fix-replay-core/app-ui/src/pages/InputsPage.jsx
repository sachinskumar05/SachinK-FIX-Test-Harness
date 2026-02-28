import { useMemo } from "react";
import { useAppState } from "../state/AppStateContext";

function Field({ label, hint, children }) {
  return (
    <label className="field">
      <span className="field-label">{label}</span>
      {children}
      {hint ? <small className="field-hint">{hint}</small> : null}
    </label>
  );
}

export default function InputsPage() {
  const { inputs, setInput, loadScenarioTemplate } = useAppState();

  const scenarioSize = useMemo(() => `${inputs.scenarioYaml.length} chars`, [inputs.scenarioYaml]);

  const downloadScenario = () => {
    const blob = new Blob([inputs.scenarioYaml], { type: "text/yaml;charset=utf-8" });
    const href = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = href;
    link.download = "scenario.yaml";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(href);
  };

  return (
    <section className="panel">
      <h2>Input Setup</h2>
      <div className="grid-two">
        <Field
          label="Backend Base URL"
          hint="Default: http://localhost:7000. All API calls use this URL."
        >
          <input
            type="text"
            value={inputs.apiBaseUrl}
            onChange={event => setInput("apiBaseUrl", event.target.value)}
          />
        </Field>

        <Field
          label="Scan Path (server-side)"
          hint="Use for POST /api/scan JSON mode. Example: C:\\logs\\session.log"
        >
          <input
            type="text"
            value={inputs.scanPath}
            onChange={event => setInput("scanPath", event.target.value)}
          />
        </Field>

        <Field
          label="Scan Upload (local file)"
          hint="If selected, upload mode is used for /api/scan."
        >
          <input
            type="file"
            accept=".log,.txt,.fix,.in,.out"
            onChange={event => setInput("scanFile", event.target.files?.[0] ?? null)}
          />
        </Field>

        <Field
          label="Scenario Path (server-side)"
          hint="Used by /api/prepare, /api/run-offline, /api/run-online."
        >
          <input
            type="text"
            value={inputs.scenarioPath}
            onChange={event => setInput("scenarioPath", event.target.value)}
          />
        </Field>

        <Field
          label="Input Folder (server-side .in)"
          hint="Used by /api/prepare."
        >
          <input
            type="text"
            value={inputs.inputFolder}
            onChange={event => setInput("inputFolder", event.target.value)}
          />
        </Field>

        <Field
          label="Expected Folder (server-side .out)"
          hint="Used by /api/prepare."
        >
          <input
            type="text"
            value={inputs.expectedFolder}
            onChange={event => setInput("expectedFolder", event.target.value)}
          />
        </Field>

        <Field
          label="Cache Folder (optional)"
          hint="Writes link-report artifacts during /api/prepare."
        >
          <input
            type="text"
            value={inputs.cacheDir}
            onChange={event => setInput("cacheDir", event.target.value)}
          />
        </Field>

        <Field
          label="JUnit Path (optional)"
          hint="Used by run endpoints to emit junit.xml."
        >
          <input
            type="text"
            value={inputs.junitPath}
            onChange={event => setInput("junitPath", event.target.value)}
          />
        </Field>
      </div>

      <div className="scenario-editor">
        <div className="scenario-header">
          <h3>Scenario YAML Editor</h3>
          <div className="button-row">
            <button type="button" onClick={loadScenarioTemplate}>
              Load Template
            </button>
            <button type="button" onClick={downloadScenario}>
              Download YAML
            </button>
          </div>
        </div>
        <p className="field-hint">
          {scenarioSize}. Editor is for authoring templates; run endpoints consume scenario path from server file system.
        </p>
        <textarea
          value={inputs.scenarioYaml}
          onChange={event => setInput("scenarioYaml", event.target.value)}
          rows={16}
        />
      </div>
    </section>
  );
}
