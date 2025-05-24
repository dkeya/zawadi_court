import hashlib 
from pathlib import Path
import getpass

print("🔐 Zawadi Court Admin Password Reset")
password = getpass.getpass("Enter a new admin password: ")

hashed_password = hashlib.sha256(password.encode()).hexdigest()
print(f"\n✅ New Password Hash: {hashed_password}")

# Save to Streamlit secrets.toml
secrets_path = Path(".streamlit/secrets.toml")
secrets_path.parent.mkdir(parents=True, exist_ok=True)

with open(secrets_path, "w") as f:
    f.write(f'ADMIN_PASSWORD_HASH = "{hashed_password}"\\n')

print(f"\n🔐 Updated secrets.toml at: {secrets_path.resolve()}")

# Existing password hash
ADMIN_PASSWORD_HASH = "your_admin_password_hash_here"

# Email settings for reminders
EMAIL_ADDRESS = "your_email@gmail.com"
EMAIL_PASSWORD = "your_16_char_app_password"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
