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
            "生产环境脚本": {
                "data.ps1": {
                    "name": "K线数据服务",
                    "description": "启动币安K线数据服务",
                    "category": "production"
                },
                "aggregate.ps1": {
                    "name": "K线聚合系统",
                    "description": "启动K线聚合服务",
                    "category": "production"
                }
            },
            "日志程序": {
                "start_logmcp.ps1": {
                    "name": "Log MCP 守护进程",
                    "description": "启动Log MCP守护进程服务",
                    "category": "logging"
                }
            }
        }
        
        # 运行中的进程
        self.running_processes = {}
        
        # 加载配置
        self.load_config()

        # 加载日志等级配置
        self.load_log_levels()

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
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # 标题
        title_label = ttk.Label(main_frame, text="K线系统启动器", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        # 左侧控制面板
        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # 编译模式选择
        mode_frame = ttk.LabelFrame(control_frame, text="编译模式", padding="5")
        mode_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.mode_var = tk.BooleanVar(value=self.release_mode)
        
        release_radio = ttk.Radiobutton(mode_frame, text="Release模式 (生产环境)", 
                                       variable=self.mode_var, value=True,
                                       command=self.on_mode_change)
        release_radio.pack(anchor=tk.W)
        
        debug_radio = ttk.Radiobutton(mode_frame, text="Debug模式 (开发调试)", 
                                     variable=self.mode_var, value=False,
                                     command=self.on_mode_change)
        debug_radio.pack(anchor=tk.W)
        
        # 模式说明
        mode_info = ttk.Label(mode_frame, text="Release: 性能优化，体积小\nDebug: 包含调试信息，便于排错", 
                             font=('Arial', 8), foreground='gray')
        mode_info.pack(anchor=tk.W, pady=(5, 0))
        
        # 日志等级设置
        log_frame = ttk.LabelFrame(control_frame, text="日志等级设置", padding="5")
        log_frame.pack(fill=tk.X, pady=(0, 10))

        # K线服务日志等级
        kline_log_frame = ttk.Frame(log_frame)
        kline_log_frame.pack(fill=tk.X, pady=2)
        ttk.Label(kline_log_frame, text="K线服务:").pack(side=tk.LEFT)
        self.kline_log_var = tk.StringVar(value="info")
        kline_log_combo = ttk.Combobox(kline_log_frame, textvariable=self.kline_log_var,
                                      values=["trace", "debug", "info", "warn", "error"],
                                      width=8, state="readonly")
        kline_log_combo.pack(side=tk.RIGHT)

        # WebLog服务日志等级
        weblog_log_frame = ttk.Frame(log_frame)
        weblog_log_frame.pack(fill=tk.X, pady=2)
        ttk.Label(weblog_log_frame, text="WebLog:").pack(side=tk.LEFT)
        self.weblog_log_var = tk.StringVar(value="info")
        weblog_log_combo = ttk.Combobox(weblog_log_frame, textvariable=self.weblog_log_var,
                                       values=["trace", "debug", "info", "warn", "error"],
                                       width=8, state="readonly")
        weblog_log_combo.pack(side=tk.RIGHT)

        # 应用日志等级按钮
        ttk.Button(log_frame, text="📝 应用日志等级",
                  command=self.apply_log_levels).pack(fill=tk.X, pady=(5, 0))

        # 全局操作按钮
        global_frame = ttk.LabelFrame(control_frame, text="全局操作", padding="5")
        global_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(global_frame, text="🔄 更新所有脚本",
                  command=self.update_all_scripts).pack(fill=tk.X, pady=2)
        ttk.Button(global_frame, text="🛑 停止所有进程",
                  command=self.stop_all_processes).pack(fill=tk.X, pady=2)
        ttk.Button(global_frame, text="📁 打开项目目录",
                  command=self.open_project_dir).pack(fill=tk.X, pady=2)
        
        # 右侧脚本列表
        script_frame = ttk.LabelFrame(main_frame, text="脚本列表", padding="10")
        script_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        script_frame.columnconfigure(0, weight=1)
        script_frame.rowconfigure(0, weight=1)
        
        # 创建Notebook用于分类显示脚本
        self.notebook = ttk.Notebook(script_frame)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 为每个分类创建标签页
        self.create_script_tabs()
        
        # 底部日志区域
        log_frame = ttk.LabelFrame(main_frame, text="运行日志", padding="5")
        log_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
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

    def create_script_tabs(self):
        """创建脚本分类标签页"""
        # 只显示生产环境脚本和日志程序，移除调试脚本和工具脚本
        allowed_categories = ["生产环境脚本", "日志程序"]

        for category_name, scripts in self.scripts.items():
            if category_name not in allowed_categories:
                continue  # 跳过不需要的分类

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
        """为单个脚本创建控制面板"""
        # 主面板
        panel = ttk.LabelFrame(parent, text=script_info['name'], padding="10")
        panel.pack(fill=tk.X, pady=5, padx=5)

        # 描述
        desc_label = ttk.Label(panel, text=script_info['description'],
                              font=('Arial', 9), foreground='gray')
        desc_label.pack(anchor=tk.W)

        # 文件名
        file_label = ttk.Label(panel, text=f"文件: {script_file}",
                              font=('Arial', 8), foreground='blue')
        file_label.pack(anchor=tk.W, pady=(2, 5))

        # 按钮框架
        button_frame = ttk.Frame(panel)
        button_frame.pack(fill=tk.X)

        # 启动按钮 - 更大更突出
        start_btn = ttk.Button(button_frame, text="🚀 启动", width=12,
                              command=lambda: self.run_script(script_file))
        start_btn.pack(side=tk.LEFT, padx=(0, 8), pady=2)

        # 编辑按钮
        edit_btn = ttk.Button(button_frame, text="📝 编辑", width=10,
                             command=lambda: self.edit_script(script_file))
        edit_btn.pack(side=tk.LEFT, padx=(0, 8), pady=2)

        # 查看按钮
        view_btn = ttk.Button(button_frame, text="👁 查看", width=10,
                             command=lambda: self.view_script(script_file))
        view_btn.pack(side=tk.LEFT, padx=(0, 8), pady=2)

        # 为重要脚本添加额外的启动按钮
        if script_file == "start_logmcp.ps1":
            # 添加第二个启动按钮，更突出
            start_btn2 = ttk.Button(button_frame, text="🔥 快速启动", width=12,
                                   command=lambda: self.run_script(script_file))
            start_btn2.pack(side=tk.RIGHT, padx=(8, 0), pady=2)

        # 状态标签
        status_label = ttk.Label(button_frame, text="就绪", foreground='green')
        status_label.pack(side=tk.RIGHT)

        # 保存状态标签引用
        setattr(self, f"status_{script_file.replace('.', '_')}", status_label)

    def on_mode_change(self):
        """编译模式改变时的处理"""
        self.release_mode = self.mode_var.get()
        self.save_config()
        mode_text = "Release" if self.release_mode else "Debug"
        self.log(f"编译模式已切换为: {mode_text}")
        self.log("✅ 编译模式已保存到统一配置文件，脚本将自动使用新模式")

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
        status_attr = f"status_{script_file.replace('.', '_')}"
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

    def load_log_levels(self):
        """加载当前的日志等级配置"""
        try:
            # 读取K线服务日志等级 (从 config/aggregate_config.toml)
            kline_level = self.read_kline_log_level()
            if hasattr(self, 'kline_log_var'):
                self.kline_log_var.set(kline_level)

            # 读取WebLog服务日志等级 (从 src/weblog/config/logging_config.toml)
            weblog_level = self.read_weblog_log_level()
            if hasattr(self, 'weblog_log_var'):
                self.weblog_log_var.set(weblog_level)

        except Exception as e:
            self.log(f"⚠️ 加载日志等级配置失败: {e}")

    def read_kline_log_level(self):
        """读取K线服务的日志等级（使用默认日志级别）"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 查找 [logging] 部分的 default_log_level
                    in_logging_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line == '[logging]':
                            in_logging_section = True
                        elif line.startswith('[') and line != '[logging]':
                            in_logging_section = False
                        elif in_logging_section and line.startswith('default_log_level'):
                            value = line.split('=')[1].strip().strip('"\'')
                            return value
            return "info"  # 默认值
        except Exception:
            return "info"

    def read_weblog_log_level(self):
        """读取WebLog服务的日志等级"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 查找 [logging.services] 部分的 weblog
                    in_services_section = False
                    for line in content.split('\n'):
                        line = line.strip()
                        if line == '[logging.services]':
                            in_services_section = True
                        elif line.startswith('[') and line != '[logging.services]':
                            in_services_section = False
                        elif in_services_section and line.startswith('weblog'):
                            value = line.split('=')[1].strip().strip('"\'')
                            return value
            return "info"  # 默认值
        except Exception:
            return "info"

    def apply_log_levels(self):
        """应用日志等级设置"""
        try:
            kline_level = self.kline_log_var.get()
            weblog_level = self.weblog_log_var.get()

            # 更新K线服务配置
            self.update_kline_log_level(kline_level)

            # 更新WebLog服务配置
            self.update_weblog_log_level(weblog_level)

            self.log(f"✅ 日志等级已更新: K线服务={kline_level}, WebLog={weblog_level}")
            messagebox.showinfo("设置成功", f"日志等级已更新:\nK线服务: {kline_level}\nWebLog: {weblog_level}")

        except Exception as e:
            self.log(f"❌ 更新日志等级失败: {e}")
            messagebox.showerror("设置失败", f"更新日志等级失败: {e}")

    def update_kline_log_level(self, level):
        """更新K线服务的日志等级（更新默认日志级别）"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 替换[logging]部分的default_log_level
                lines = content.split('\n')
                in_logging_section = False
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped == '[logging]':
                        in_logging_section = True
                    elif stripped.startswith('[') and stripped != '[logging]':
                        in_logging_section = False
                    elif in_logging_section and stripped.startswith('default_log_level'):
                        lines[i] = f'default_log_level = "{level}"'
                        break

                # 写回文件
                with open(self.kline_config_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                self.log(f"✅ K线服务日志等级已更新为: {level}")
            else:
                self.log(f"⚠️ 配置文件不存在: {self.kline_config_file}")

        except Exception as e:
            raise Exception(f"更新K线服务日志等级失败: {e}")

    def update_weblog_log_level(self, level):
        """更新WebLog服务的日志等级"""
        try:
            if os.path.exists(self.kline_config_file):
                with open(self.kline_config_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 替换[logging.services]部分的weblog
                lines = content.split('\n')
                in_services_section = False
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped == '[logging.services]':
                        in_services_section = True
                    elif stripped.startswith('[') and stripped != '[logging.services]':
                        in_services_section = False
                    elif in_services_section and stripped.startswith('weblog'):
                        lines[i] = f'weblog = "{level}"'
                        break

                # 写回文件
                with open(self.kline_config_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                self.log(f"✅ WebLog服务日志等级已更新为: {level}")
            else:
                self.log(f"⚠️ 配置文件不存在: {self.kline_config_file}")

        except Exception as e:
            raise Exception(f"更新WebLog服务日志等级失败: {e}")

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
