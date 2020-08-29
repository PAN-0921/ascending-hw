#-*- coding: utf-8 -*-
#keys and rooms
#https://leetcode.com/problems/keys-and-rooms/

#[False]*4得到[False,False,False,False]
#if not i: 如果i是false，那么not i就是ture，则要执行if；如果i是ture，not i就是false，则不需要执行if
#stack(后进先出)和queue（先进先出）都是list，区别是使用方法。若是stack，则list.pop（）;若是queue，则是list.pop(0)
#注意return ture的位置。在这个位置才可以保证for循环全部走完之后执行return true，与题意相符。


def do(room,index,visited):
	for i in room[index]:
		if not visited[i]:
			visited[i]=True
			do(room,i,visited)

def keysandrooms(room):
	visited=[False]*len(room)
	visited[0]=True
	do(room,0,visited)

	for i in visited:
		if not i:
			return False
	return True

print(keysandrooms([[1],[2],[3],[]]))
	
