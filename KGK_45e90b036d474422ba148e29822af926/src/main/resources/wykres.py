import numpy as np
from matplotlib import pyplot as plt


imiona=[]
miasta=[]
imiona2=[]
miasta2=[]
count =[]
file=open('wynik.txt')
for line in file:
    (m,i,c)=(line.split('|'))
    imiona.append(i)
    miasta.append(m)
    count.append(c)

imiona2 = imiona.copy()
miasta2 = miasta.copy()

for i in range(len(imiona2)):                                                            # usuniecie powtorek
    indeks=0
    if(not i>=len(imiona2)):
        for j in range(len(imiona2)):
            if imiona2[i] == imiona2[j]:
                indeks = j
                break
        if i!=indeks:
            for k in range(i,len(imiona2)):
                if k+1<len(imiona2):
                    if  imiona2[k]!=imiona2[k+1]:
                        imiona2[k]=imiona2[k+1]
                    else:
                        imiona2 = np.delete(imiona2,k+1)
                        break
    else:
        imiona2 = np.delete(imiona2,-1)
        break


for i in range(len(miasta2)):
    indeks=0
    if (not i >= len(miasta2)):
        for j in range(len(miasta2)):
            if miasta2[i] == miasta2[j]:
                indeks = j
                break
        if i!=indeks:
            for k in range(i,len(miasta2)-1):
                if miasta2[k]!=miasta2[k+1]:
                    miasta2[k]=miasta2[k+1]
                else:
                    miasta2 = np.delete(miasta2,k+1)
                    break
    else:
        miasta2 = np.delete(miasta2,-1)
        break

x_imiona=[]
x_miasta=[]
x_count=np.zeros((len(imiona),len(miasta)))

print(len(imiona2),len(miasta2),len(count))

for i in range(len(imiona2)):                                                             # policzenie dlugosci osi
    x_imiona=np.append(x_imiona,i)
for i in range(len(miasta2)):
    x_miasta=np.append(x_miasta,i)

print(len(x_imiona),len(x_miasta))
for i in range(len(imiona)):
    indeks=0
    for spr in range(len(imiona2)):                                          # sprawdzenie czy nie ma wczesniej danego imienia
        if imiona[i]==imiona2[spr]:
            indeks=spr
            break
    for j in range(len(miasta)):
        indeks2=0
        for spr2 in range(len(miasta2)):                                     # sprawdzenie czy nie ma wczesniej danego miasta
            if miasta[j] == miasta2[spr2]:
                indeks2 = spr2
                break
        if i==j:
            x_count[indeks][indeks2]=count[i]

x_count2=np.zeros((len(imiona2),len(miasta2)))
for i in range(len(imiona)):
    if i<len(imiona2):
        print(i)
        for j in range(len(miasta)):
            if j<len(miasta2):
                print(j)
                x_count2[i][j] = x_count[i][j]


plt.figure(figsize=(5,5),dpi=100)
plt.imshow(x_count2,interpolation="none")
plt.yticks(x_imiona,imiona2)
plt.xticks(x_miasta,miasta2,rotation=90)
plt.grid()
plt.show()

plt.figure(figsize=(20,20),dpi=200)
plt.imshow(x_count2,interpolation="none")
plt.yticks(x_imiona,imiona2)
plt.xticks(x_miasta,miasta2,rotation=90)
plt.grid()
plt.savefig("wykres.pdf")