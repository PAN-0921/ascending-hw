#hw4
#Binary Tree(BFS)
class Node(object):
	def __init__(self,val):
		self.val=val
		self.right=None
		self.left=None

a=Node(1)
b=Node(2)
c=Node(3)
d=Node(4)
e=Node(5)
a.left=b
a.right=c
b.left=d
b.right=e

def bfs(node):
	q=[node]
	while len(q)>0:
		size=len(q)
		for i in range(size):
			cur=q.pop(0)
			print(cur.val)
			if cur.left:
				q.append(cur.left)
			if cur.right:
				q.append(cur.right)



print(bfs(a))
