@echo off
echo Cleaning up...

set xdir=%1
IF NOT "%xdir%" == "" goto dir_is_set
set xdir=.
:dir_is_set

del %xdir%\*.ylib 2>nul
del %xdir%\*.xlib 2>nul
del %xdir%\*.zb 2>nul
del %xdir%\zmap_*.zm 2>nul
del %xdir%\temp_*-????-????-????-*.pdb 2>nul
del %xdir%\temp_*-????-????-????-*.dll 2>nul

@rem   Now that killall cleans up, don't want to kill the error logs...
@rem del %xdir%\errors.txt 2>nul
@rem del %xdir%\slave-errors.txt 2>nul
@rem del %xdir%\slave-log.txt 2>nul
@rem del %xdir%\aelight-errors.txt 2>nul
@rem del %xdir%\error.cs 2>nul
