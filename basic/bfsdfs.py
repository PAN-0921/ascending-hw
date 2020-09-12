class Node():
        def __init__(self, val):
		self.val=val
        	self.left=None
		self.right=None
a = Node(1)  
b = Node(2)
c = Node(3)
d = Node(4)
e = Node(5)

a.left=b
a.right=c
b.left=d
b.right=e

#dfs-find target number
def dfs_findtargetnumber(tar,node):
	if node==None:
		return False
	if node.val==tar:
		return True	
	res1=dfs_findtargetnumber(tar,node.left)
        res2=dfs_findtargetnumber(tar,node.right)		
	return res1 or res2
	
print(dfs_findtargetnumber(5,e))

#bfs-find target number
def bfs_findtargetnumber(tar,node):
	q=[]
	q.append(node)
	while len(q)>0:
		i=q.pop(0)
		if i.val==tar:
			return True
        	if i.left!=None:
			q.append(i.left)
		if i.right!=None:
			q.append(i.right)
	return False
print(bfs_findtargetnumber(5,e))



