To view the visualization, enter data into raw.txt and run 

```
python process.py > links.csv
python processNodes.py > nodes.csv
```

then type:

```
python -m SimpleHTTPServer 4321
```

Then go into a web browser and type `http://localhost:4321/` into the URL bar.
