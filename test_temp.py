# Test the mitmproxy addon behavior
code_without_requests = """# This file was generated using mitmproxy
from aiolocust import HttpUser


async def run(self: HttpUser):
"""

print("Code without any requests:")
print(repr(code_without_requests))
print(f"Valid Python: {True if check_syntax(code_without_requests) else False}")

def check_syntax(code):
    try:
        compile(code, 'test.py', 'exec')
        return True
    except SyntaxError:
        return False
