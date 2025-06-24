#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trace 日志分析工具
分析 TraceDistiller 生成的函数执行路径快照，提供详细的性能分析报告
"""

import sys
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class TraceNode:
    """表示调用树中的一个节点"""
    name: str
    duration_ms: Optional[float] = None
    self_time_ms: Optional[float] = None
    is_critical_path: bool = False
    has_error: bool = False
    fields: Dict[str, str] = None
    children: List['TraceNode'] = None
    depth: int = 0

    def __post_init__(self):
        if self.fields is None:
            self.fields = {}
        if self.children is None:
            self.children = []

class TraceAnalyzer:
    """Trace 日志分析器"""

    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.traces: List[Dict] = []
        self.root_nodes: List[TraceNode] = []


    def parse_log_file(self) -> bool:
        """解析日志文件"""
        if not self.log_file.exists():
            print(f"❌ 文件不存在: {self.log_file}")
            return False

        print(f"📖 正在解析文件: {self.log_file}")

        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # 按 Trace 分割内容
            trace_sections = re.split(r'------------------ End of Trace [0-9a-fx]+ ------------------', content)

            for section in trace_sections:
                if '=== 函数执行路径分析报告 ===' in section:
                    trace_info = self._parse_trace_section(section)
                    if trace_info:
                        self.traces.append(trace_info)

            print(f"✅ 成功解析 {len(self.traces)} 个 Trace")
            return True

        except Exception as e:
            print(f"❌ 解析文件失败: {e}")
            return False

    def _parse_trace_section(self, section: str) -> Optional[Dict]:
        """解析单个 Trace 段落"""
        lines = section.strip().split('\n')
        if not lines:
            return None

        trace_info = {
            'trace_id': None,
            'root_function': None,
            'total_duration': None,
            'status': None,
            'call_tree': None
        }

        # 解析头部信息
        for line in lines:
            if 'Trace ID:' in line:
                trace_info['trace_id'] = line.split('Trace ID:')[1].strip()
            elif '根函数:' in line:
                trace_info['root_function'] = line.split('根函数:')[1].strip()
            elif '总耗时:' in line:
                duration_match = re.search(r'(\d+\.?\d*)ms', line)
                if duration_match:
                    trace_info['total_duration'] = float(duration_match.group(1))
            elif '执行状态:' in line:
                trace_info['status'] = '成功' if '✅' in line else '失败'
            elif '=== 调用树结构 ===' in line:
                # 找到调用树开始位置，跳过格式说明行
                tree_start_idx = lines.index(line)
                # 跳过格式说明行，找到真正的调用树开始
                actual_start = tree_start_idx + 1
                while actual_start < len(lines):
                    line_content = lines[actual_start].strip()
                    # 跳过格式说明和空行
                    if (line_content and
                        not line_content.startswith('格式:') and
                        not line_content.startswith('🔥') and
                        not line_content.startswith('❌') and
                        '=' not in line_content and
                        line_content != ''):
                        break
                    actual_start += 1

                tree_lines = lines[actual_start:]
                trace_info['call_tree'] = self._parse_call_tree(tree_lines)
                break

        return trace_info if trace_info['trace_id'] else None


    def _parse_call_tree(self, tree_lines: List[str]) -> TraceNode:
        """解析调用树结构"""
        if not tree_lines:
            return None

        root = None
        node_stack = []  # (node, depth)

        # 开始解析调用树

        for line_num, line in enumerate(tree_lines):
            if not line.strip() or line.startswith('===') or '错误信息汇总' in line:
                break

            node = self._parse_tree_line(line)
            if node is None:
                continue

            if root is None:
                root = node
                node_stack = [(root, node.depth)]
                if not hasattr(self, 'root_nodes'):
                    self.root_nodes = []
                self.root_nodes.append(root)  # 添加到根节点列表
            else:
                # 找到正确的父节点
                while node_stack and node_stack[-1][1] >= node.depth:
                    node_stack.pop()

                if node_stack:
                    parent = node_stack[-1][0]
                    parent.children.append(node)
                else:
                    # 如果没有合适的父节点，可能是新的根节点
                    if not hasattr(self, 'root_nodes'):
                        self.root_nodes = []
                    self.root_nodes.append(node)

                node_stack.append((node, node.depth))

        return root
    def _parse_tree_line(self, line: str) -> Optional[TraceNode]:
        """解析调用树的单行"""
        original_line = line

        # 移除树形连接符，但保留缩进信息
        # 树形连接符包括: │ ├ └ ─ 以及空格
        clean_line = re.sub(r'^(\s*)[│├└─\s]*', r'\1', line)

        # 计算实际的缩进深度（基于原始缩进）
        leading_spaces = len(line) - len(line.lstrip())
        # 每3个字符为一层缩进，但需要考虑树形连接符的影响
        # 树形连接符通常占用3个字符位置：'   ', '├─ ', '└─ ', '│  '
        depth = leading_spaces // 3

        content = clean_line.strip()
        if not content:
            return None

        # 解析标记
        is_critical = '🔥' in content
        has_error = '❌' in content
        content = re.sub(r'[🔥❌]\s*', '', content)

        # 解析函数名和时间信息
        # 格式: function_name (123.45ms | 67.89ms) [param=value]
        match = re.match(r'([^\(]+)\s*\(([^)]+)\)(?:\s*\[([^\]]+)\])?', content)

        if not match:
            # 如果正则匹配失败，尝试简单解析
            simple_match = re.match(r'([^\s\(]+)', content)
            if simple_match:
                name = simple_match.group(1).strip()
                return TraceNode(
                    name=name,
                    duration_ms=None,
                    self_time_ms=None,
                    is_critical_path=is_critical,
                    has_error=has_error,
                    fields={},
                    depth=depth
                )
            return None

        name = match.group(1).strip()
        timing_info = match.group(2)
        fields_info = match.group(3) if match.group(3) else ""

        # 解析时间信息
        duration_ms = None
        self_time_ms = None

        if '|' in timing_info:
            parts = timing_info.split('|')
            duration_match = re.search(r'(-?\d+\.?\d*)ms', parts[0])
            self_time_match = re.search(r'(-?\d+\.?\d*)ms', parts[1])

            if duration_match:
                duration_ms = float(duration_match.group(1))
            if self_time_match:
                self_time_ms = float(self_time_match.group(1))
        else:
            duration_match = re.search(r'(-?\d+\.?\d*)ms', timing_info)
            if duration_match:
                duration_ms = float(duration_match.group(1))

        # 解析字段信息
        fields = {}
        if fields_info:
            for field in fields_info.split(','):
                if '=' in field:
                    key, value = field.split('=', 1)
                    fields[key.strip()] = value.strip()

        return TraceNode(
            name=name,
            duration_ms=duration_ms,
            self_time_ms=self_time_ms,
            is_critical_path=is_critical,
            has_error=has_error,
            fields=fields,
            depth=depth
        )

    def analyze(self) -> Dict:
        """执行分析并返回结果"""
        if not self.traces:
            return {"error": "没有找到有效的 Trace 数据"}

        analysis = {
            "summary": self._generate_summary(),
            "performance_analysis": self._analyze_performance(),
            "error_analysis": self._analyze_errors(),
            "function_statistics": self._analyze_functions(),
            "recommendations": self._generate_recommendations()
        }

        return analysis

    def _generate_summary(self) -> Dict:
        """生成总体摘要"""
        total_traces = len(self.traces)
        successful_traces = sum(1 for t in self.traces if t['status'] == '成功')
        failed_traces = total_traces - successful_traces

        total_duration = sum(t['total_duration'] for t in self.traces if t['total_duration'])
        avg_duration = total_duration / len([t for t in self.traces if t['total_duration']]) if total_duration else 0

        return {
            "total_traces": total_traces,
            "successful_traces": successful_traces,
            "failed_traces": failed_traces,
            "success_rate": f"{(successful_traces/total_traces*100):.1f}%" if total_traces > 0 else "0%",
            "total_duration_ms": total_duration,
            "average_duration_ms": avg_duration
        }

    def _analyze_performance(self) -> Dict:
        """分析性能数据"""
        durations = [t['total_duration'] for t in self.traces if t['total_duration']]

        if not durations:
            return {"error": "没有有效的性能数据"}

        durations.sort()

        return {
            "min_duration_ms": min(durations),
            "max_duration_ms": max(durations),
            "median_duration_ms": durations[len(durations)//2],
            "p95_duration_ms": durations[int(len(durations)*0.95)] if len(durations) > 1 else durations[0],
            "slowest_traces": self._get_slowest_traces(3)
        }

    def _analyze_errors(self) -> Dict:
        """分析错误信息"""
        failed_traces = [t for t in self.traces if t['status'] == '失败']

        return {
            "total_errors": len(failed_traces),
            "error_rate": f"{(len(failed_traces)/len(self.traces)*100):.1f}%" if self.traces else "0%",
            "failed_functions": [t['root_function'] for t in failed_traces]
        }

    def _analyze_functions(self) -> Dict:
        """分析函数统计"""
        function_stats = {}

        for trace in self.traces:
            if trace['call_tree']:
                self._collect_function_stats(trace['call_tree'], function_stats)

        # 按调用次数排序
        sorted_functions = sorted(function_stats.items(), key=lambda x: x[1]['count'], reverse=True)

        return {
            "total_unique_functions": len(function_stats),
            "most_called_functions": sorted_functions[:10],
            "function_details": function_stats
        }

    def _collect_function_stats(self, node: TraceNode, stats: Dict):
        """递归收集函数统计信息"""
        if node.name not in stats:
            stats[node.name] = {
                'count': 0,
                'total_duration': 0,
                'total_self_time': 0,
                'error_count': 0
            }

        stats[node.name]['count'] += 1
        if node.duration_ms:
            stats[node.name]['total_duration'] += node.duration_ms
        if node.self_time_ms:
            stats[node.name]['total_self_time'] += node.self_time_ms
        if node.has_error:
            stats[node.name]['error_count'] += 1

        for child in node.children:
            self._collect_function_stats(child, stats)

    def _get_slowest_traces(self, count: int) -> List[Dict]:
        """获取最慢的几个 Trace"""
        sorted_traces = sorted(
            [t for t in self.traces if t['total_duration']],
            key=lambda x: x['total_duration'],
            reverse=True
        )

        return sorted_traces[:count]

    def _generate_recommendations(self) -> List[str]:
        """生成优化建议"""
        recommendations = []

        # 基于分析结果生成建议
        durations = [t['total_duration'] for t in self.traces if t['total_duration']]
        if durations:
            max_duration = max(durations)
            avg_duration = sum(durations) / len(durations)

            if max_duration > avg_duration * 3:
                recommendations.append(f"⚠️  发现异常慢的执行路径，最慢耗时 {max_duration:.2f}ms，建议检查性能瓶颈")

            if avg_duration > 1000:
                recommendations.append(f"⚠️  平均执行时间较长 ({avg_duration:.2f}ms)，建议优化关键路径")

        failed_count = len([t for t in self.traces if t['status'] == '失败'])
        if failed_count > 0:
            recommendations.append(f"❌ 发现 {failed_count} 个失败的执行路径，建议检查错误处理")

        if not recommendations:
            recommendations.append("✅ 系统运行正常，未发现明显问题")

        return recommendations

    def print_analysis(self, analysis: Dict):
        """打印分析结果"""
        print("\n" + "="*60)
        print("📊 TRACE 分析报告")
        print("="*60)

        # 总体摘要
        summary = analysis['summary']
        print(f"\n📈 总体摘要:")
        print(f"  总 Trace 数量: {summary['total_traces']}")
        print(f"  成功执行: {summary['successful_traces']} ({summary['success_rate']})")
        print(f"  执行失败: {summary['failed_traces']}")
        print(f"  总耗时: {summary['total_duration_ms']:.2f}ms")
        print(f"  平均耗时: {summary['average_duration_ms']:.2f}ms")

        # 性能分析
        if 'error' not in analysis['performance_analysis']:
            perf = analysis['performance_analysis']
            print(f"\n⚡ 性能分析:")
            print(f"  最快执行: {perf['min_duration_ms']:.2f}ms")
            print(f"  最慢执行: {perf['max_duration_ms']:.2f}ms")
            print(f"  中位数: {perf['median_duration_ms']:.2f}ms")
            print(f"  95分位数: {perf['p95_duration_ms']:.2f}ms")

            print(f"\n🐌 最慢的执行路径:")
            for i, trace in enumerate(perf['slowest_traces'], 1):
                print(f"  {i}. {trace['root_function']} - {trace['total_duration']:.2f}ms")

        # 错误分析
        errors = analysis['error_analysis']
        if errors['total_errors'] > 0:
            print(f"\n❌ 错误分析:")
            print(f"  错误数量: {errors['total_errors']} ({errors['error_rate']})")
            print(f"  失败的函数: {', '.join(errors['failed_functions'])}")

        # 函数统计
        func_stats = analysis['function_statistics']
        print(f"\n🔧 函数统计:")
        print(f"  唯一函数数量: {func_stats['total_unique_functions']}")
        print(f"  调用最频繁的函数:")
        for func_name, stats in func_stats['most_called_functions'][:5]:
            avg_duration = stats['total_duration'] / stats['count'] if stats['count'] > 0 else 0
            print(f"    {func_name}: {stats['count']}次调用, 平均{avg_duration:.2f}ms")

        # 优化建议
        print(f"\n💡 优化建议:")
        for rec in analysis['recommendations']:
            print(f"  {rec}")

        print("\n" + "="*60)

def main():
    if len(sys.argv) != 2:
        print("用法: python analyze_trace.py <trace_log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    analyzer = TraceAnalyzer(log_file)

    if not analyzer.parse_log_file():
        sys.exit(1)

    analysis = analyzer.analyze()
    analyzer.print_analysis(analysis)

if __name__ == "__main__":
    main()