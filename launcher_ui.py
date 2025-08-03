#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线系统启动器UI
管理所有PowerShell脚本的启动，支持debug/release模式切换
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import subprocess
import threading
import os
import re
from pathlib import Path
import json
from datetime import datetime

class KlineSystemLauncher:
    def __init__(self, root):
        self.root = root
        self.root.title("K线系统启动器 - Kline System Launcher")
        self.root.geometry("1000x700")
        self.root.configure(bg='#f0f0f0')
        
        # 配置文件路径
        self.config_file = "launcher_config.json"
        self.kline_config_file = "config/BinanceKlineConfig.toml"
        
        # 脚本配置
        self.scripts = {
            "核心服务": {
                "data.ps1": {
                    "name": "K线数据服务",
                    "file": "data.ps1",
                    "category": "production"
                },
                "klagg_sub.ps1": {
                    "name": "K线聚合系统(测试)",
                    "file": "klagg_sub.ps1",
                    "category": "development"
                },
                "klagg_sub_prod.ps1": {
                    "name": "K线聚合系统(生产)",
                    "file": "klagg_sub_prod.ps1",
                    "category": "production"
                }
            },
            "审计与调试": {
                "klagg_visual_test": {
                    "name": "可视化审计模式",
                    "file": "cargo run --bin klagg_visual_test",
                    "category": "audit",
                    "is_cargo": True
                },
                "scripts\\klagg_memory_analysis.ps1": {
                    "name": "内存分析工具",
                    "file": "scripts\\klagg_memory_analysis.ps1",
                    "category": "analysis"
                }
            },
            "日志与监控": {
                "start_logmcp.ps1": {
                    "name": "Log MCP守护进程",
                    "file": "start_logmcp.ps1",
                    "category": "logging"
                },
                "src\\weblog\\start_weblog_with_window.ps1": {
                    "name": "WebLog可视化系统",
                    "file": "src\\weblog\\start_weblog_with_window.ps1",
                    "category": "logging"
                }
            },
            "数据工具": {
                "scripts\\kline_integrity_checker.ps1": {
                    "name": "数据完整性检查器",
                    "file": "scripts\\kline_integrity_checker.ps1",
                    "category": "tools"
                }
            }
        }
        
        # 运行中的进程
        self.running_processes = {}
        
        # 加载配置
        self.load_config()

        # 加载日志配置
        self.load_log_config()

        # 创建UI
        self.create_ui()
        
    def load_config(self):
        """加载配置文件"""
        try:
            # 从统一配置文件读取编译模式
            self.release_mode = self.read_build_mode_from_config()
        except Exception as e:
            self.release_mode = False  # 默认为debug模式
            print(f"加载配置失败: {e}")

    def read_build_mode_from_config(self):
        """从BinanceKlineConfig.toml读取编译模式"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 查找 [build] 部分的 mode 配置
                    in_build_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line == '[build]':
                            in_build_section = True
                        elif line.startswith('[') and line != '[build]':
                            in_build_section = False
                        elif in_build_section and line.startswith('mode'):
                            # 提取模式值
                            value = line.split('=')[1].strip().strip('"\'')
                            return value.lower() == "release"
            return False  # 默认为debug模式
        except Exception:
            return False
    
    def save_config(self):
        """保存配置文件"""
        try:
            # 保存到统一配置文件
            self.save_build_mode_to_config()

            # 同时保存到launcher配置文件（保持兼容性）
            config = {
                'release_mode': self.release_mode,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存配置失败: {e}")

    def save_build_mode_to_config(self):
        """保存编译模式到BinanceKlineConfig.toml"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 替换[build]部分的mode配置
                lines = content.split('\n')
                in_build_section = False
                mode_value = "release" if self.release_mode else "debug"

                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped == '[build]':
                        in_build_section = True
                    elif stripped.startswith('[') and stripped != '[build]':
                        in_build_section = False
                    elif in_build_section and stripped.startswith('mode'):
                        lines[i] = f'mode = "{mode_value}"'
                        break

                # 写回文件
                with open(self.kline_config_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                self.log(f"✅ 编译模式已保存到配置文件: {mode_value}")
            else:
                self.log(f"⚠️ 配置文件不存在: {self.kline_config_file}")

        except Exception as e:
            raise Exception(f"保存编译模式失败: {e}")
    
    def create_ui(self):
        """创建用户界面"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)  # 脚本列表区域可扩展

        # 标题
        title_label = ttk.Label(main_frame, text="K线系统启动器",
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, pady=(0, 10))

        # 顶部控制面板
        control_frame = ttk.LabelFrame(main_frame, text="系统设置", padding="10")
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        control_frame.columnconfigure(0, weight=1)

        # 设置行框架
        settings_row = ttk.Frame(control_frame)
        settings_row.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        settings_row.columnconfigure(2, weight=1)

        # 编译模式选择
        mode_frame = ttk.LabelFrame(settings_row, text="编译模式", padding="5")
        mode_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))

        self.mode_var = tk.BooleanVar(value=self.release_mode)

        release_radio = ttk.Radiobutton(mode_frame, text="Release (生产)",
                                       variable=self.mode_var, value=True,
                                       command=self.on_mode_change)
        release_radio.pack(anchor=tk.W)

        debug_radio = ttk.Radiobutton(mode_frame, text="Debug (调试)",
                                     variable=self.mode_var, value=False,
                                     command=self.on_mode_change)
        debug_radio.pack(anchor=tk.W)

        # 审计模式选择
        audit_frame = ttk.LabelFrame(settings_row, text="审计模式", padding="5")
        audit_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))

        # 从配置文件读取审计状态
        initial_audit_state = self.read_audit_config()
        self.audit_var = tk.BooleanVar(value=initial_audit_state)

        audit_check = ttk.Checkbutton(audit_frame, text="启用审计功能",
                                     variable=self.audit_var,
                                     command=self.on_audit_change)
        audit_check.pack(anchor=tk.W)

        audit_info = ttk.Label(audit_frame, text="包含生命周期校验\n和数据完整性审计",
                              font=('Arial', 8), foreground='gray')
        audit_info.pack(anchor=tk.W, pady=(2, 0))
        
        # 日志设置
        log_frame = ttk.LabelFrame(settings_row, text="日志设置", padding="5")
        log_frame.grid(row=0, column=2, sticky=(tk.W, tk.E))

        # 日志开关和等级在一行
        log_row1 = ttk.Frame(log_frame)
        log_row1.pack(fill=tk.X, pady=(0, 5))

        self.log_enabled_var = tk.BooleanVar(value=True)
        log_enable_check = ttk.Checkbutton(log_row1, text="启用日志",
                                          variable=self.log_enabled_var,
                                          command=self.on_log_enable_change)
        log_enable_check.pack(side=tk.LEFT)

        ttk.Label(log_row1, text="等级:").pack(side=tk.LEFT, padx=(10, 5))
        self.kline_log_var = tk.StringVar(value="info")
        kline_log_combo = ttk.Combobox(log_row1, textvariable=self.kline_log_var,
                                      values=["trace", "debug", "info", "warn", "error"],
                                      width=8, state="readonly")
        kline_log_combo.pack(side=tk.LEFT)

        # 应用按钮
        ttk.Button(log_frame, text="应用设置",
                  command=self.apply_log_settings).pack(fill=tk.X, pady=(5, 0))

        # 全局操作按钮行
        global_row = ttk.Frame(control_frame)
        global_row.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        ttk.Button(global_row, text="🔄 更新配置",
                  command=self.update_all_scripts).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(global_row, text="🛑 停止所有进程",
                  command=self.stop_all_processes).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(global_row, text="📁 打开项目目录",
                  command=self.open_project_dir).pack(side=tk.LEFT)
        
        # 脚本列表
        script_frame = ttk.LabelFrame(main_frame, text="服务启动", padding="10")
        script_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        script_frame.columnconfigure(0, weight=1)
        script_frame.rowconfigure(0, weight=1)

        # 创建Notebook用于分类显示脚本
        self.notebook = ttk.Notebook(script_frame)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 为每个分类创建标签页
        self.create_script_tabs()
        
        # 底部日志区域
        log_frame = ttk.LabelFrame(main_frame, text="运行日志", padding="5")
        log_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 日志控制按钮
        log_control_frame = ttk.Frame(log_frame)
        log_control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        
        ttk.Button(log_control_frame, text="清空日志", 
                  command=self.clear_log).pack(side=tk.LEFT)
        ttk.Button(log_control_frame, text="保存日志", 
                  command=self.save_log).pack(side=tk.LEFT, padx=(5, 0))
        
        # 初始化日志
        self.log("K线系统启动器已启动")
        self.log(f"当前模式: {'Release' if self.release_mode else 'Debug'}")
        if hasattr(self, 'audit_var'):
            audit_status = "启用" if self.audit_var.get() else "禁用"
            self.log(f"审计功能: {audit_status} (从配置文件读取)")
        else:
            self.log("审计功能: 禁用 (默认)")

    def create_script_tabs(self):
        """创建脚本分类标签页"""
        for category_name, scripts in self.scripts.items():

            # 创建标签页框架
            tab_frame = ttk.Frame(self.notebook)
            self.notebook.add(tab_frame, text=category_name)

            # 创建滚动框架
            canvas = tk.Canvas(tab_frame)
            scrollbar = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)

            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )

            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)

            # 为每个脚本创建控制面板
            for script_file, script_info in scripts.items():
                self.create_script_panel(scrollable_frame, script_file, script_info)

            # 布局滚动组件
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

    def create_script_panel(self, parent, script_file, script_info):
        """为单个脚本创建控制面板 - 一行布局"""
        # 主框架 - 一行布局
        panel = ttk.Frame(parent, padding="5")
        panel.pack(fill=tk.X, pady=2, padx=5)
        panel.columnconfigure(1, weight=1)  # 文件名列可扩展

        # 脚本名称 (固定宽度)
        name_label = ttk.Label(panel, text=script_info['name'], width=20, anchor='w')
        name_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 10))

        # 文件名 (可扩展)
        file_text = script_info.get('file', script_file)
        file_label = ttk.Label(panel, text=file_text, font=('Arial', 9),
                              foreground='blue', anchor='w')
        file_label.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))

        # 启动按钮
        if script_info.get('is_cargo', False):
            # Cargo命令需要特殊处理
            start_btn = ttk.Button(panel, text="🚀 启动", width=10,
                                  command=lambda: self.run_cargo_command(script_file, script_info))
        else:
            start_btn = ttk.Button(panel, text="� 启动", width=10,
                                  command=lambda: self.run_script(script_info['file']))
        start_btn.grid(row=0, column=2, padx=(0, 5))

        # 状态标签
        status_label = ttk.Label(panel, text="就绪", foreground='green', width=8)
        status_label.grid(row=0, column=3)

        # 保存状态标签引用
        status_key = script_file.replace('.', '_').replace('\\', '_').replace('/', '_')
        setattr(self, f"status_{status_key}", status_label)

    def on_audit_change(self):
        """审计模式改变时的处理"""
        audit_enabled = self.audit_var.get()
        self.log(f"审计模式已{'启用' if audit_enabled else '禁用'}")

        # 更新配置文件
        try:
            self.update_audit_config(audit_enabled)
            if audit_enabled:
                self.log("✅ 审计功能包含：生命周期事件校验、数据完整性审计")
                self.log("✅ 配置文件已更新：enable_audit = true")
            else:
                self.log("⚠️ 审计功能已禁用，将使用零成本抽象模式")
                self.log("✅ 配置文件已更新：enable_audit = false")
        except Exception as e:
            self.log(f"❌ 更新配置文件失败: {e}")

    def update_audit_config(self, audit_enabled):
        """更新配置文件中的审计开关"""
        config_path = "config\\BinanceKlineConfig.toml"

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # 查找并更新 enable_audit 行
        updated = False
        for i, line in enumerate(lines):
            if line.strip().startswith('enable_audit'):
                lines[i] = f"enable_audit = {str(audit_enabled).lower()}\n"
                updated = True
                break

        if not updated:
            raise ValueError("配置文件中未找到 enable_audit 配置项")

        # 写回配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

    def read_audit_config(self):
        """从配置文件读取审计开关状态"""
        config_path = "config\\BinanceKlineConfig.toml"

        try:
            if not os.path.exists(config_path):
                return False  # 默认禁用

            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 查找 enable_audit 配置
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('enable_audit'):
                    if '=' in line:
                        value = line.split('=')[1].strip()
                        return value.lower() == 'true'

            return False  # 默认禁用

        except Exception as e:
            self.log(f"⚠️ 读取审计配置失败: {e}")
            return False

    def run_cargo_command(self, script_key, script_info):
        """运行Cargo命令"""
        file_path = script_info['file']

        # 检查是否启用审计模式
        if script_key == "klagg_visual_test":
            # 重新从配置文件读取审计状态，确保与配置文件同步
            current_audit_state = self.read_audit_config()
            self.log(f"📋 从配置文件读取审计状态: {'启用' if current_audit_state else '禁用'}")

            if current_audit_state:
                # 审计模式：使用 --features full-audit
                cmd_parts = file_path.split()
                if "--features" not in cmd_parts:
                    cmd_parts.extend(["--features", "full-audit"])
                cmd = cmd_parts
                self.log(f"🔍 启动审计模式: {' '.join(cmd)}")
                self.log("✅ 审计功能已启用 - 程序将包含生命周期校验和数据完整性审计")
            else:
                # 普通模式
                cmd = file_path.split()
                self.log(f"🚀 启动可视化测试: {' '.join(cmd)}")
                self.log("⚠️ 审计功能已禁用 - 程序将使用零成本抽象模式")
        else:
            cmd = file_path.split()
            self.log(f"🚀 启动命令: {' '.join(cmd)}")

        # 更新状态
        status_key = script_key.replace('.', '_').replace('\\', '_').replace('/', '_')
        status_attr = f"status_{status_key}"
        if hasattr(self, status_attr):
            status_label = getattr(self, status_attr)
            status_label.config(text="运行中", foreground='orange')

        def run_in_thread():
            try:
                process = subprocess.Popen(
                    cmd,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )

                # 保存进程引用
                self.running_processes[script_key] = process

                # 等待进程完成
                process.wait()

                # 更新状态
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    if process.returncode == 0:
                        status_label.config(text="完成", foreground='green')
                        self.log(f"✅ 命令执行完成: {script_key}")
                    else:
                        status_label.config(text="错误", foreground='red')
                        self.log(f"❌ 命令执行失败: {script_key}")

                # 移除进程引用
                if script_key in self.running_processes:
                    del self.running_processes[script_key]

            except Exception as e:
                self.log(f"❌ 启动命令失败: {script_key}, 错误: {e}")
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    status_label.config(text="错误", foreground='red')

                if script_key in self.running_processes:
                    del self.running_processes[script_key]

        # 在新线程中运行
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

    def on_mode_change(self):
        """编译模式改变时的处理"""
        self.release_mode = self.mode_var.get()
        self.save_config()
        mode_text = "Release" if self.release_mode else "Debug"
        self.log(f"编译模式已切换为: {mode_text}")
        self.log("✅ 编译模式已保存到统一配置文件，脚本将自动使用新模式")

    def on_log_enable_change(self):
        """日志开关变化时的处理"""
        log_enabled = self.log_enabled_var.get()

        # 启用/禁用日志等级设置控件
        if log_enabled:
            # 启用所有子控件
            for child in self.log_levels_frame.winfo_children():
                self.enable_widget_recursive(child)
            self.log("✅ 日志系统已启用")
        else:
            # 禁用所有子控件
            for child in self.log_levels_frame.winfo_children():
                self.disable_widget_recursive(child)
            self.log("⚠️ 日志系统已禁用 - 所有程序将不输出日志")

        # 保存配置
        self.save_log_enable_config()

    def enable_widget_recursive(self, widget):
        """递归启用控件及其子控件"""
        try:
            widget.configure(state='normal')
        except:
            try:
                widget.configure(state='readonly')  # 对于Combobox
            except:
                pass

        # 递归处理子控件
        for child in widget.winfo_children():
            self.enable_widget_recursive(child)

    def disable_widget_recursive(self, widget):
        """递归禁用控件及其子控件"""
        try:
            widget.configure(state='disabled')
        except:
            pass

        # 递归处理子控件
        for child in widget.winfo_children():
            self.disable_widget_recursive(child)

    def log(self, message):
        """添加日志消息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"

        self.log_text.insert(tk.END, log_message)
        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)

    def save_log(self):
        """保存日志到文件"""
        try:
            log_content = self.log_text.get(1.0, tk.END)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"launcher_log_{timestamp}.txt"

            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(log_content)

            self.log(f"日志已保存到: {log_file}")
            messagebox.showinfo("保存成功", f"日志已保存到: {log_file}")
        except Exception as e:
            self.log(f"保存日志失败: {e}")
            messagebox.showerror("保存失败", f"保存日志失败: {e}")

    def run_script(self, script_file):
        """运行PowerShell脚本"""
        if not os.path.exists(script_file):
            self.log(f"❌ 脚本文件不存在: {script_file}")
            messagebox.showerror("文件不存在", f"脚本文件不存在: {script_file}")
            return

        self.log(f"🚀 启动脚本: {script_file}")

        # 更新状态
        status_key = script_file.replace('.', '_').replace('\\', '_').replace('/', '_')
        status_attr = f"status_{status_key}"
        if hasattr(self, status_attr):
            status_label = getattr(self, status_attr)
            status_label.config(text="运行中", foreground='orange')

        def run_in_thread():
            try:
                # 使用PowerShell运行脚本
                cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", script_file]

                process = subprocess.Popen(
                    cmd,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )

                # 保存进程引用
                self.running_processes[script_file] = process

                # 等待进程完成
                process.wait()

                # 更新状态
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    if process.returncode == 0:
                        status_label.config(text="完成", foreground='green')
                        self.log(f"✅ 脚本执行完成: {script_file}")
                    else:
                        status_label.config(text="错误", foreground='red')
                        self.log(f"❌ 脚本执行失败: {script_file}")
                        self.log(f"返回码: {process.returncode}")

                # 移除进程引用
                if script_file in self.running_processes:
                    del self.running_processes[script_file]

            except Exception as e:
                self.log(f"❌ 启动脚本失败: {script_file}, 错误: {e}")
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    status_label.config(text="错误", foreground='red')

                # 移除进程引用
                if script_file in self.running_processes:
                    del self.running_processes[script_file]

        # 在新线程中运行
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()



    def run_memory_analysis(self, duration_seconds):
        """运行内存分析（指定时长）"""
        script_file = "scripts\\klagg_memory_analysis.ps1"

        if not os.path.exists(script_file):
            self.log(f"❌ 脚本文件不存在: {script_file}")
            messagebox.showerror("文件不存在", f"脚本文件不存在: {script_file}")
            return

        self.log(f"🔍 启动内存分析 ({duration_seconds}秒): {script_file}")

        # 更新状态
        status_attr = f"status_{script_file.replace('.', '_').replace('\\\\', '_')}"
        if hasattr(self, status_attr):
            status_label = getattr(self, status_attr)
            status_label.config(text=f"分析中({duration_seconds}s)", foreground='orange')

        def run_in_thread():
            try:
                # 使用PowerShell运行脚本，添加-Duration参数
                cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", script_file, "-Duration", str(duration_seconds)]

                process = subprocess.Popen(
                    cmd,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )

                # 保存进程引用
                process_key = f"{script_file}_memory_{duration_seconds}"
                self.running_processes[process_key] = process

                # 等待进程完成
                process.wait()

                # 更新状态
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    if process.returncode == 0:
                        status_label.config(text="分析完成", foreground='green')
                        self.log(f"✅ 内存分析完成 ({duration_seconds}秒): {script_file}")
                        self.log(f"📄 分析结果已保存到 dhat-heap.json")
                        self.log(f"🌐 请访问 https://nnethercote.github.io/dhat/viewer/ 查看结果")
                    else:
                        status_label.config(text="分析失败", foreground='red')
                        self.log(f"❌ 内存分析失败 ({duration_seconds}秒): {script_file}")
                        self.log(f"返回码: {process.returncode}")

                # 移除进程引用
                if process_key in self.running_processes:
                    del self.running_processes[process_key]

            except Exception as e:
                self.log(f"❌ 启动内存分析失败: {script_file}, 错误: {e}")
                if hasattr(self, status_attr):
                    status_label = getattr(self, status_attr)
                    status_label.config(text="错误", foreground='red')

                # 移除进程引用
                process_key = f"{script_file}_memory_{duration_seconds}"
                if process_key in self.running_processes:
                    del self.running_processes[process_key]

        # 在新线程中运行
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

    def edit_script(self, script_file):
        """编辑PowerShell脚本"""
        try:
            # 使用系统默认编辑器打开文件
            if os.name == 'nt':  # Windows
                os.startfile(script_file)
            else:  # Linux/Mac
                subprocess.run(['xdg-open', script_file])

            self.log(f"📝 打开编辑器: {script_file}")
        except Exception as e:
            self.log(f"❌ 打开编辑器失败: {e}")
            messagebox.showerror("打开失败", f"无法打开编辑器: {e}")

    def view_script(self, script_file):
        """查看PowerShell脚本内容"""
        try:
            if not os.path.exists(script_file):
                messagebox.showerror("文件不存在", f"脚本文件不存在: {script_file}")
                return

            # 创建查看窗口
            view_window = tk.Toplevel(self.root)
            view_window.title(f"查看脚本 - {script_file}")
            view_window.geometry("800x600")

            # 创建文本框
            text_frame = ttk.Frame(view_window, padding="10")
            text_frame.pack(fill=tk.BOTH, expand=True)

            text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.NONE)
            text_widget.pack(fill=tk.BOTH, expand=True)

            # 读取并显示文件内容
            with open(script_file, 'r', encoding='utf-8') as f:
                content = f.read()
                text_widget.insert(1.0, content)

            # 设置为只读
            text_widget.config(state=tk.DISABLED)

            self.log(f"👁 查看脚本: {script_file}")

        except Exception as e:
            self.log(f"❌ 查看脚本失败: {e}")
            messagebox.showerror("查看失败", f"无法查看脚本: {e}")

    def update_all_scripts(self):
        """更新配置文件（不再需要更新脚本）"""
        self.log("ℹ️ 使用统一配置文件，无需更新脚本")
        self.log("✅ 脚本将自动从配置文件读取编译模式")
        messagebox.showinfo("配置更新", "编译模式已保存到统一配置文件\n脚本将自动使用新的编译模式")

    def update_script_compile_mode(self, script_file):
        """已废弃：不再需要更新脚本，使用统一配置文件"""
        self.log(f"ℹ️ 跳过脚本更新: {script_file} (使用统一配置文件)")
        return True

    def stop_all_processes(self):
        """停止所有运行中的进程"""
        if not self.running_processes:
            self.log("ℹ️ 没有运行中的进程")
            return

        self.log("🛑 停止所有运行中的进程...")

        stopped_count = 0
        for script_file, process in list(self.running_processes.items()):
            try:
                if process.poll() is None:  # 进程仍在运行
                    process.terminate()
                    process.wait(timeout=5)
                    stopped_count += 1
                    self.log(f"✅ 已停止: {script_file}")

                    # 更新状态
                    status_attr = f"status_{script_file.replace('.', '_')}"
                    if hasattr(self, status_attr):
                        status_label = getattr(self, status_attr)
                        status_label.config(text="已停止", foreground='gray')

                del self.running_processes[script_file]

            except subprocess.TimeoutExpired:
                # 强制终止
                process.kill()
                stopped_count += 1
                self.log(f"⚠️ 强制终止: {script_file}")
                del self.running_processes[script_file]
            except Exception as e:
                self.log(f"❌ 停止进程失败 {script_file}: {e}")

        self.log(f"✅ 已停止 {stopped_count} 个进程")

    def open_project_dir(self):
        """打开项目目录"""
        try:
            if os.name == 'nt':  # Windows
                os.startfile('.')
            else:  # Linux/Mac
                subprocess.run(['xdg-open', '.'])

            self.log("📁 已打开项目目录")
        except Exception as e:
            self.log(f"❌ 打开项目目录失败: {e}")
            messagebox.showerror("打开失败", f"无法打开项目目录: {e}")

    def load_log_config(self):
        """加载当前的日志配置"""
        try:
            # 读取日志开关状态
            log_enabled = self.read_log_enabled()
            if hasattr(self, 'log_enabled_var'):
                self.log_enabled_var.set(log_enabled)

            # 读取K线服务日志等级
            kline_level = self.read_kline_log_level()
            if hasattr(self, 'kline_log_var'):
                self.kline_log_var.set(kline_level)

            # 根据日志开关状态设置控件状态
            if hasattr(self, 'log_levels_frame'):
                if log_enabled:
                    for child in self.log_levels_frame.winfo_children():
                        self.enable_widget_recursive(child)
                else:
                    for child in self.log_levels_frame.winfo_children():
                        self.disable_widget_recursive(child)

        except Exception as e:
            self.log(f"⚠️ 加载日志配置失败: {e}")

    def read_log_enabled(self):
        """读取日志开关状态"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 查找 [logging] 部分的 enabled
                    in_logging_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line == '[logging]':
                            in_logging_section = True
                        elif line.startswith('[') and line != '[logging]':
                            in_logging_section = False
                        elif in_logging_section and line.startswith('enabled'):
                            value = line.split('=')[1].strip().strip('"\'')
                            return value.lower() == 'true'
            return True  # 默认启用
        except Exception:
            return True

    def read_kline_log_level(self):
        """读取K线服务的日志等级"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 查找 [logging] 部分的 log_level
                    in_logging_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line == '[logging]':
                            in_logging_section = True
                        elif line.startswith('[') and line != '[logging]':
                            in_logging_section = False
                        elif in_logging_section and line.startswith('log_level'):
                            value = line.split('=')[1].strip().strip('"\'')
                            return value
            return "info"  # 默认值
        except Exception:
            return "info"



    def save_log_enable_config(self):
        """保存日志开关配置"""
        try:
            log_enabled = self.log_enabled_var.get()
            self.update_log_enabled(log_enabled)
            self.log(f"✅ 日志开关已保存: {'启用' if log_enabled else '禁用'}")
        except Exception as e:
            self.log(f"❌ 保存日志开关失败: {e}")

    def apply_log_settings(self):
        """应用日志设置"""
        try:
            log_enabled = self.log_enabled_var.get()
            kline_level = self.kline_log_var.get()

            # 更新日志开关
            self.update_log_enabled(log_enabled)

            # 更新K线服务配置
            self.update_kline_log_level(kline_level)

            status_text = "启用" if log_enabled else "禁用"
            self.log(f"✅ 日志设置已更新: 状态={status_text}, 日志等级={kline_level}")
            messagebox.showinfo("设置成功",
                              f"日志设置已更新:\n状态: {status_text}\n日志等级: {kline_level}")

        except Exception as e:
            self.log(f"❌ 更新日志设置失败: {e}")
            messagebox.showerror("设置失败", f"更新日志设置失败: {e}")

    def update_log_enabled(self, enabled):
        """更新日志开关状态"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 替换[logging]部分的enabled配置
                lines = content.split('\n')
                in_logging_section = False
                enabled_found = False

                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped == '[logging]':
                        in_logging_section = True
                    elif stripped.startswith('[') and stripped != '[logging]':
                        in_logging_section = False
                    elif in_logging_section and stripped.startswith('enabled'):
                        lines[i] = f'enabled = {str(enabled).lower()}'
                        enabled_found = True
                        break

                # 如果没有找到enabled配置，在[logging]部分添加
                if not enabled_found:
                    for i, line in enumerate(lines):
                        stripped = line.strip()
                        if stripped == '[logging]':
                            # 在[logging]行后插入enabled配置
                            lines.insert(i + 1, f'enabled = {str(enabled).lower()}')
                            break

                # 写回文件
                with open(self.kline_config_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                self.log(f"✅ 日志开关已更新为: {'启用' if enabled else '禁用'}")
            else:
                self.log(f"⚠️ 配置文件不存在: {self.kline_config_file}")

        except Exception as e:
            raise Exception(f"更新日志开关失败: {e}")

    def update_kline_log_level(self, level):
        """更新K线服务的日志等级"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 替换[logging]部分的log_level
                lines = content.split('\n')
                in_logging_section = False
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped == '[logging]':
                        in_logging_section = True
                    elif stripped.startswith('[') and stripped != '[logging]':
                        in_logging_section = False
                    elif in_logging_section and stripped.startswith('log_level'):
                        lines[i] = f'log_level = "{level}"'
                        break

                # 写回文件
                with open(self.kline_config_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                self.log(f"✅ K线服务日志等级已更新为: {level}")
            else:
                self.log(f"⚠️ 配置文件不存在: {self.kline_config_file}")

        except Exception as e:
            raise Exception(f"更新K线服务日志等级失败: {e}")




def main():
    """主函数"""
    # 检查是否在项目根目录
    if not os.path.exists("Cargo.toml"):
        messagebox.showerror("错误",
                           "请在项目根目录运行此程序\n"
                           "当前目录应包含 Cargo.toml 文件")
        return

    # 创建主窗口
    root = tk.Tk()

    # 设置窗口图标（如果有的话）
    try:
        # root.iconbitmap('icon.ico')  # 如果有图标文件
        pass
    except:
        pass

    # 创建应用实例
    app = KlineSystemLauncher(root)

    # 运行主循环
    root.mainloop()

if __name__ == "__main__":
    main()
