@echo off
REM ============================================================================
REM  dcm_migrate CLI wrapper.  Usage:  dcm_migrate <command> [options]
REM  e.g.  dcm_migrate init   /   dcm_migrate --config migration.toml doctor
REM  Prefers `uv` (resolves pydicom/pynetdicom from the script's PEP 723 header);
REM  falls back to the `py` launcher (needs pydicom + pynetdicom installed).
REM ============================================================================
setlocal
where uv >nul 2>nul
if %errorlevel%==0 (
  uv run "%~dp0dcm_migrate.py" %*
) else (
  py -3 "%~dp0dcm_migrate.py" %*
)
endlocal
