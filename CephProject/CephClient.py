import requests
import os

url = "http://172.16.3.207:8080"


def printcommands():
    print("Please choose one of the following commands and insert the right parameters:\n"
          "commands -> prints the list of available commands;\n"
          "ls -> prints the list of available files in the storage\n"
          "delete <filename> -> deletes the file with the corresponding filename\n"
          "upload <filepath> -> uploads the file from the corresponding file path\n"
          "download <filename> -> downloads the file with the corresponding filename\n"
          "stats -> shows the current statistics on the status of the cluster\n"
          "exit -> closes the client\n")


if __name__ == '__main__':
    printcommands()

    while True:
        prompt = input("> ")
        tokens = prompt.split()
        if len(tokens) < 1:
            continue

        command = tokens[0]

        if command == "commands":
            printcommands()

        elif command == "ls":
            r = requests.get("{}/v1/files".format(url))
            print(r.text)

        elif command == "delete":
            filename = tokens[1]
            r = requests.delete("{}/v1/files/{}".format(url, filename))
            print(r.text)

        elif command == "upload":
            filepath = tokens[1]
            file = open(filepath, 'rb')
            files = {'file': file}
            r = requests.post("{}/v1/files".format(url), files=files)
            print(r.text)

        elif command == "download":
            filename = tokens[1]
            r = requests.get("{}/v1/files/{}".format(url, filename))
            if r.status_code == 200:
                with open(filename, "wb") as file:
                    file.write(r.content)
                print("File received successfully and saved in: {}".format(filename))
            else:
                print(r.text)

        elif command == "stats":
            r = requests.get("{}/v1/stats".format(url))
            print(r.text)

        elif command == "exit":
            break
