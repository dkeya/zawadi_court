-- Minimal demo rows (remove in production)
INSERT INTO contributions (house_no, family_name, lane, rate_category, email)
VALUES ('A01', 'Family Alpha', 'ROYAL', 'Resident', 'alpha@example.com')
ON CONFLICT DO NOTHING;

INSERT INTO expenses (date, description, category, vendor, amount_kes, mode)
VALUES (CURRENT_DATE, 'Gate Repair', 'Maintenance', 'John Doe', 5000, 'Cash');

INSERT INTO special_contributions (event, date, type, contributors, amount)
VALUES ('Baby Shower', CURRENT_DATE, 'Celebration', 'Family Alpha', 3000);
