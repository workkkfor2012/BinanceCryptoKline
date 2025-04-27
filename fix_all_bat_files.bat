@echo off
chcp 65001 > nul
echo 正在为所有批处理文件添加UTF-8支持...
python check\convert_bat_encoding.py --add-utf8 *.bat check\*.bat
echo.
echo 处理完成！
pause
