import random

def generate(size: int = 30):
	return f'%{size:03}x' % random.randrange(16**size)

print(generate(128))