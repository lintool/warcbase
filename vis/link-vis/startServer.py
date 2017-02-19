import SimpleHTTPServer, SocketServer
import glob, os, json
import argparse

# get list of files under /data/
listOfJsonFiles = []
for filename in os.listdir("data"):
	if filename.endswith(".json"):
		listOfJsonFiles.append(filename)

# update variables.js
varFileContent = ""
with open("assets/js/variables.temp", "r") as f:
	varFileContent = f.read()
with open("assets/js/variables.js", "w") as f:
	replacedContent = varFileContent.format(listOfAvailableDataFiles=json.dumps(listOfJsonFiles))
	f.write(replacedContent)

# get commandline arguments
parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=4321)
args = parser.parse_args()

Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
httpd = SocketServer.TCPServer(("", args.port), Handler)
print "Start server at port", args.port
httpd.serve_forever()

