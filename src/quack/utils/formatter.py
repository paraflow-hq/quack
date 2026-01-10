#!/usr/bin/env python3


def format_duration(seconds: float) -> str:
    """格式化时间duration为易读格式

    Args:
        seconds: 秒数

    Returns:
        格式化后的字符串,如 "100ms", "1.23s", "2m30.50s"
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m{secs:.2f}s"


def format_size(size_bytes: int) -> str:
    """格式化文件大小为易读格式

    Args:
        size_bytes: 字节数

    Returns:
        格式化后的字符串,如 "1.23 KB", "45.67 MB"
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"
