import json

if __name__="__main__":
    x = 0

    asin = []

    with open('Movies_and_TV_5.json', 'r') as f:
        while 1:
            line = f.readline()
            if not line:
                break
            data = json.loads(line)
            asin.append(data["asin"])
            x += 1


    asin = list(set(asin))
    print(len(asin))

    print("done")
    print(x)