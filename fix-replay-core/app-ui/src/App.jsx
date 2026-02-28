import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import InputsPage from "./pages/InputsPage";
import RunPage from "./pages/RunPage";
import ResultsPage from "./pages/ResultsPage";
import { useAppState } from "./state/AppStateContext";

const navItems = [
  { to: "/inputs", label: "Inputs" },
  { to: "/run", label: "Run" },
  { to: "/results", label: "Results" }
];

export default function App() {
  const { loading, error, lastOperation, jobSnapshot } = useAppState();

  return (
    <div className="app-shell">
      <header className="top-bar">
        <h1>FIX Replay Harness</h1>
        <div className="status-row">
          <span className="chip">Last operation: {lastOperation ?? "none"}</span>
          <span className={`chip ${loading ? "chip-busy" : ""}`}>{loading ? "Running" : "Idle"}</span>
          {jobSnapshot ? (
            <span className={`chip chip-job chip-${jobSnapshot.status.toLowerCase()}`}>
              Job {jobSnapshot.jobId}: {jobSnapshot.status}
            </span>
          ) : null}
        </div>
      </header>

      <nav className="main-nav">
        {navItems.map(item => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) => (isActive ? "nav-link nav-link-active" : "nav-link")}
          >
            {item.label}
          </NavLink>
        ))}
      </nav>

      <main className="content">
        {error ? <section className="panel error-panel">{error}</section> : null}
        <Routes>
          <Route path="/" element={<Navigate to="/inputs" replace />} />
          <Route path="/inputs" element={<InputsPage />} />
          <Route path="/run" element={<RunPage />} />
          <Route path="/results" element={<ResultsPage />} />
        </Routes>
      </main>
    </div>
  );
}
