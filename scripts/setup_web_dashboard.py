"""Create a local least-privilege PostgreSQL login for the web dashboard."""

from __future__ import annotations

import secrets
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PASSWORD_FILE = ROOT / "config/secrets/web_dashboard_db_password.txt"
VIEWS = (
    "vw_post_period", "vw_hashtag_period", "vw_hashtag_pair_period",
    "vw_topic_period", "vw_topic_hashtag", "vw_topic_sound",
    "vw_engagement_observation", "vw_coverage", "vw_publication",
)


def main() -> None:
    PASSWORD_FILE.parent.mkdir(parents=True, exist_ok=True)
    password = secrets.token_urlsafe(36)
    PASSWORD_FILE.write_text(password + "\n", encoding="utf-8")
    try:
        PASSWORD_FILE.chmod(0o600)
    except OSError:
        pass
    sql = [
        "DO $role$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='web_dashboard') THEN CREATE ROLE web_dashboard LOGIN; END IF; END $role$;",
        f"ALTER ROLE web_dashboard WITH LOGIN PASSWORD '{password.replace(chr(39), chr(39) * 2)}';",
        "GRANT CONNECT ON DATABASE trend_detect TO web_dashboard;",
        "GRANT USAGE ON SCHEMA vitied TO web_dashboard;",
        *[f"GRANT SELECT ON vitied.{view} TO web_dashboard;" for view in VIEWS],
    ]
    result = subprocess.run(
        ["docker", "compose", "-f", "infra/compose.yaml", "-f", "infra/compose.powerbi.yaml", "exec", "-T", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "trend_detect"],
        input="\n".join(sql) + "\n", text=True, capture_output=True, cwd=ROOT,
    )
    if result.returncode:
        PASSWORD_FILE.unlink(missing_ok=True)
        raise SystemExit(result.stderr.strip() or "Could not create web dashboard database role")
    print("Created database role web_dashboard with SELECT access to the nine vitied views.")
    print(f"Local database secret saved at {PASSWORD_FILE.relative_to(ROOT)} (gitignored).")


if __name__ == "__main__":
    main()
