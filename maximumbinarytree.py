# -*- coding: utf-8 -*-
#Maximum Binary Tree
#https://leetcode.com/problems/maximum-binary-tree/description/
#思路：反向dfs
#enumerate列举:
#my_list = ['apple', 'banana', 'grapes', 'pear']
#for counter, value in enumerate(my_list):
#print counter, value
# Output:
# 0 apple
# 1 banana
# 2 grapes
# 3 pear


class Node:
	def __init__(self,val):
		self.val=val
		self.left=None
		self.right=None

def findmaxindex(array):
	re=-1
	maxval=float('-inf')
	for index,value in enumerate(array):
		if value>maxval:
			maxval=value
			re=index
	return re
#当程序运行到lefttree时开始recursive，全部recursive完之后再开始下一步righttree.
#每当开始一个新的def时会出现root，lefttree，righttree，但是当运行到return时才会真的输出一个root。因此，这道题目二叉树的构建是逆向的，即从最底层开始。

def constructmaximumbinarytree(array):
	if array==[]:
		return None
	if len(array)==1:
		return Node(array[0])

	maxindex=findmaxindex(array)
	root=Node(array[maxindex])
	lefttree=constructmaximumbinarytree(array[:maxindex])
	righttree=constructmaximumbinarytree(array[maxindex+1:])

	root.left=lefttree
	root.right=righttree
	return root

def dfs(root):
	print root.val
	if root.left:
		dfs(root.left)
	if root.right:
		dfs(root.right)

dfs(constructmaximumbinarytree([3,2,1,6,0,5]))
