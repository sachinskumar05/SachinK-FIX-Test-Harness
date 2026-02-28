export default function SummaryCards({ cards }) {
  if (!cards || cards.length === 0) {
    return null;
  }

  return (
    <section className="summary-grid">
      {cards.map(card => (
        <article key={card.label} className="summary-card">
          <span className="summary-label">{card.label}</span>
          <strong className="summary-value">{card.value}</strong>
        </article>
      ))}
    </section>
  );
}
