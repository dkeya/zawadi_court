# scripts/test_reads.py
from app.queries import list_contributions, list_expenses, get_summary_totals

rows = list_contributions(search=None, limit=5)
print(f"Contributions sample ({len(rows)}):")
for r in rows:
    print(r["house_no"], r["family_name"], r["current_debt"])

exp = list_expenses(limit=3)
print(f"\nExpenses sample ({len(exp)}):")
for r in exp:
    print(r["date"], r["description"], r["amount_kes"])

tot = get_summary_totals()
print("\nTotals:", tot)
