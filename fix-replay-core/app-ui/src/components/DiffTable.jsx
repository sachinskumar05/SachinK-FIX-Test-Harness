export default function DiffTable({ rows, selectedRowId, onSelectRow }) {
  if (!rows || rows.length === 0) {
    return <p className="field-hint">No diff rows for the current response/filter.</p>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
        <tr>
          <th>ID</th>
          <th>Session</th>
          <th>MsgType</th>
          <th>Status</th>
          <th>Tags</th>
        </tr>
        </thead>
        <tbody>
        {rows.map(row => (
          <tr
            key={row.id}
            className={row.id === selectedRowId ? "selected-row" : ""}
            onClick={() => onSelectRow(row)}
          >
            <td>{row.id}</td>
            <td>{row.session}</td>
            <td>{row.msgType}</td>
            <td>{row.passed ? "PASS" : "FAIL"}</td>
            <td>{row.allTags.join(", ") || "-"}</td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  );
}
