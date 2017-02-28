We have a built in link visualizer, built in D3.js. This was developed by the amazing team of Alice Ran Zhou (University of Waterloo), Jeremy Wiebe (University of Waterloo), Shane Martin (York University), and Eric Oosenbrug (York University), in part during the [Archives Unleashed hackathon](http://archivesunleashed.ca/). You can see their commit history in [this repository](https://github.com/shamrt/link-structure).

To view the visualization, enter JSON data into `data` directory and run

```
python startServer.py --port 4321
```

Then go into a web browser and type `http://localhost:4321/` into the URL bar. You can select any of the JSON files contained in that directory.
