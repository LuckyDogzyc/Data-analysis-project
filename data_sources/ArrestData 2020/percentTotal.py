import sys

array = []
for line in sys.stdin:
	line = line.strip()
	array.append(line)

for i in range(1, len(array)):
	date, count = array[i].split(",")
	prevDate, prevCount = array[i-1].split(",")
	print(date+","+str((float(count) - float(prevCount))/float(prevCount)))
