import re

file = open("/home/chenjieli/Desktop/CaJaDe/src/log_output.txt", "r")

f = file.read()

reg_rc_line = re.compile(r'EC T_ProjectionOperator.*\nList size [0-9]+\n({.*})')

rc_line = reg_rc_line.search(f).group(1)
 
res = re.findall(r'(\{.*?\})', rc_line)

print(res)

# rc_tokens = [x.strip('{').strip('}') for x in rcs.search(f).group(1).split(' ')]

# print(rc_tokens)

