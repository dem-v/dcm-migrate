@echo off
REM ============================================================================
REM  dcm_migrate control panel (GUI) — double-click to launch.
REM  Requires Python 3.11+ with tkinter.  Prefers `uv` (which fetches a suitable
REM  Python automatically); falls back to the `py` launcher.
REM  Pass a config path as the first argument, else migration.toml is used.
REM ============================================================================
setlocal
where uv >nul 2>nul
if %errorlevel%==0 (
  uv run --python 3.12 "%~dp0dcm_migrate_gui.py" %*
) else (
  py -3 "%~dp0dcm_migrate_gui.py" %*
)
if %errorlevel% neq 0 (
  echo.
  echo Launch failed. Ensure either `uv` or Python 3.11+ ^(with tkinter^) is installed.
  pause
)
endlocal
