import { toTagDetailRows } from "../utils/diffRows";

export default function DiffDetail({ diffRow }) {
  if (!diffRow) {
    return <p className="field-hint">Select a diff row to inspect expected vs actual tags.</p>;
  }

  const detailRows = toTagDetailRows(diffRow);
  if (detailRows.length === 0) {
    return <p className="field-hint">No differences in selected row.</p>;
  }

  return (
    <section className="diff-detail">
      <h3>Expected vs Actual: {diffRow.id}</h3>
      <div className="side-by-side">
        <div>
          <h4>Expected</h4>
          <ul className="kv-list">
            {detailRows.map((row, index) => (
              <li key={`exp-${row.tag}-${index}`}>
                <span className="tag">{row.tag}</span>
                <span>{row.expected}</span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <h4>Actual</h4>
          <ul className="kv-list">
            {detailRows.map((row, index) => (
              <li key={`act-${row.tag}-${index}`}>
                <span className="tag">{row.tag}</span>
                <span>{row.actual}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
