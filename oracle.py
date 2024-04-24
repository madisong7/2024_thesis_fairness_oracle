import csv

csv_filename = "test2.csv"
log_filename_prefix = "nodelog"
numberOfNodes = 3
gamma = 2.0/3.0


csv_file = open(csv_filename, "r")

def parse_log(filename, numNodes):
  # open all the log file, stored in a list for easy access
  log_files = []
  for i in range(1, numNodes+1):
    log_files.append(open(filename+str(i)+".txt", "r"))

  # create dictionary holding all the recieve and output times for each node grouped by message
  messageData = {}
  for n in range(0, numNodes):
  nodeData = []
  for line in log_files[n]:
    # search for receive log
    if "Pooled new future transaction" in line:
      # extract timestamp
      time = line.split("|")[1].split("]")[0].split(":")
      # convert the time into number of seconds since midnight (for easy comparison)
      time = float(time[0])*3600.0 + float(time[1])*60.0 + float(time[2])
      # extract message hash
      message = line.split("hash=")[1].split()[0]

      # if we have not yet encountered this message, add a new dictionary entry
      if message not in messageData:
      # default all nodes to -1, this means a node has not yet recieved/output a message
        messageData[message] = [-1] * (numNodes*2)
      # add data to the dictionary
      messageData[message][n] = time

      # search for output log
    if "Removed old pending transaction" in line:
      # extract timestamp
      time = line.split("|")[1].split("]")[0].split(":")
      # convert the time into number of seconds since midnight (for easy comparison)
      time = float(time[0])*3600.0 + float(time[1])*60.0 + float(time[2])
      # extract message hash
      message = line.split("hash=")[1].split()[0]
      messageData[message][numNodes+n] = time

  # rearrange the messageData to be in the right form
  data = []
  testcase_index = 0
  output_first = [0]*numNodes
  # convert from dict to list to make iteration easier
  messageDataList = [x for x in messageData.items()]
  for message1 in range(len(messageDataList)):
    _, m1 = messageDataList[message1]
    for message2 in range(message1+1, len(messageDataList)):
      _, m2 = messageDataList[message2]
      for n in range(numNodes):
        if m1[numNodes+n] == -1:
          if m2[numNodes+n] == -1:
            output_first[n] = -1
          else:
            output_first[n] = 2
        elif m1[numNodes+n] < m2[numNodes+n] or m2[numNodes+n] == -1:
          output_first[n] = 1
        else:
          output_first[n] = 2
      data.append([testcase_index] + m1[:numNodes] + m2[:numNodes] + output_first)
      testcase_index += 1

  for file in log_files:
    file.close()

  return data

def generateTestInput(filename, data):
    # A function to create a CSV directly using the data given
    file = open(filename, 'w', newline='')
    writer = csv.writer(file)

    for row in data:
        writer.writerow(row)
    file.close()

def parse_csv(file):
    reader = csv.reader(file)
    data = []
    for row in reader:
        data.append(row)
    return data

def oracle(data, gamma, numNodes):
    results = []
    for row in data:
        #print(row)
        receiveTimes = row[1:-numNodes]

        #number of nodes that have received both m1 and m2
        receivedNum = numNodes
        #number of nodes that received m1 before m2
        receivedM1 = 0
        #number of nodes that received m2 before m1
        receivedM2 = 0
        for n in range(0, numNodes):
            # row[n] is the time that node n received m1, row[n+numNodes] is the time that node n received m2
            # if node n has received both, add 1 if node n received m1 before m2
            if row[n] == "-1" or row[n+numNodes] == "-1":
                receivedNum -= 1
            elif float(row[n]) <= float(row[n+numNodes]):
                receivedM1 += 1
            else:
                receivedM2 += 1

        if receivedM1 >= gamma*float(receivedNum):
            #m1 must be delivered before m2
            #If any node delievered m2 before m1, fairness is false
            if "2" in row[-numNodes:]:
                results.append(0)
            elif "-1" in row[-numNodes:]:
                results.append(-1)
            else:
                results.append(1)
        elif receivedM2 >= gamma*float(receivedNum):
            #m2 must be delivered before m1
            if "1" in row[-numNodes:]:
                results.append(0)
            elif "-1" in row[-numNodes:]:
                results.append(-1)
            else:
                results.append(1)
        else:
            #fairness cannot yet be determined for this pair of messages at this time
            results.append(-1)
    return results


# parse the logs, then create the CSV from the output
generateTestInput(csv_filename, parse_log(log_filename_prefix, numberOfNodes))

# run the oracle and print the results
print(oracle(parse_csv(csv_file), gamma, numberOfNodes))

csv_file.close()
