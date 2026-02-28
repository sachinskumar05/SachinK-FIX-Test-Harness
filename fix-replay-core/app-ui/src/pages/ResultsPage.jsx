import { useEffect, useMemo, useState } from "react";
import DiffDetail from "../components/DiffDetail";
import DiffTable from "../components/DiffTable";
import SummaryCards from "../components/SummaryCards";
import { useAppState } from "../state/AppStateContext";
import { buildSummaryCards, filterDiffRows, toDiffRows } from "../utils/diffRows";

export default function ResultsPage() {
  const { resultPayload, lastOperation } = useAppState();
  const [filters, setFilters] = useState({ session: "all", msgType: "all", tag: "" });
  const [selectedRow, setSelectedRow] = useState(null);

  const rows = useMemo(() => toDiffRows(resultPayload), [resultPayload]);
  const cards = useMemo(
    () => buildSummaryCards(resultPayload, lastOperation),
    [resultPayload, lastOperation]
  );
  const filteredRows = useMemo(() => filterDiffRows(rows, filters), [rows, filters]);

  const sessions = useMemo(
    () => ["all", ...new Set(rows.map(row => row.session))],
    [rows]
  );
  const msgTypes = useMemo(
    () => ["all", ...new Set(rows.map(row => row.msgType))],
    [rows]
  );

  useEffect(() => {
    setSelectedRow(null);
  }, [resultPayload]);

  const updateFilter = (name, value) => {
    setFilters(current => ({ ...current, [name]: value }));
  };

  return (
    <section className="panel">
      <h2>Results</h2>
      {!resultPayload ? <p className="field-hint">Run an operation to populate results.</p> : null}
      <SummaryCards cards={cards} />

      <section className="panel inset">
        <h3>Diff Filters</h3>
        <div className="grid-three">
          <label className="field">
            <span className="field-label">Session</span>
            <select
              value={filters.session}
              onChange={event => updateFilter("session", event.target.value)}
            >
              {sessions.map(item => (
                <option value={item} key={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="field-label">MsgType</span>
            <select
              value={filters.msgType}
              onChange={event => updateFilter("msgType", event.target.value)}
            >
              {msgTypes.map(item => (
                <option value={item} key={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="field-label">Tag</span>
            <input
              type="text"
              value={filters.tag}
              onChange={event => updateFilter("tag", event.target.value)}
              placeholder="e.g. 11"
            />
          </label>
        </div>
      </section>

      <section className="panel inset">
        <h3>Diff Table</h3>
        <DiffTable rows={filteredRows} selectedRowId={selectedRow?.id} onSelectRow={setSelectedRow} />
      </section>

      <section className="panel inset">
        <DiffDetail diffRow={selectedRow} />
      </section>

      <section className="panel inset">
        <h3>Raw JSON</h3>
        <pre className="json-block">{JSON.stringify(resultPayload, null, 2)}</pre>
      </section>
    </section>
  );
}
