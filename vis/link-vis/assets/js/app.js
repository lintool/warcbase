// Global variables
// ---------------------------------

// TODO: this better
var width = document.getElementById("graph").offsetWidth;
var height = document.getElementById("graph").offsetHeight;

var all_links = [];
var all_nodes = [];

var node_to_links = {};
var node_by_name = {};

var threshold = 100;

var max_links = 1;
var num_nodes = 1;

var min_year = 0;
var max_year = 0;

// setup D3 graph
var graph = d3.select("#graph")
  .attr("style", "width:"+width+"px;height:"+height+"px")
  .append("svg")
    .attr("width", width)
    .attr("height", height);

var links = graph.selectAll("line");
var node = graph.selectAll("g.node");

// force-directed layout settings
var force = d3.layout.force()
    .size([width, height])
    .friction(0.2);

var drag = force.drag()
  .on("dragstart", dragstart);

// graph controls
var min_zoom = 0.1;
var max_zoom = 7;
var zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom]);

// D3 tooltips
var nodetip = d3.tip()
  .attr('class', 'd3-tip')
  .offset([-10, 0])
  .html(function (d) {
    return "<div class='popup-tip'>" +
      "Domain: " + d.name + "<br>" +
      "PageRank: " + d.pageRank + "<br>" +
      "Links in: " + d.inDegree + "<br>" +
      "Links out: " + d.outDegree + "<br>" +
      "Total links: " + d.count + "<br>" +
    "</div>";
  });

var linktip = d3.tip()
  .attr('class', 'd3-tip')
  .offset([-10, 0])
  .html(function (l) {
    return "<div class='popup-tip'>" +
        "Link" + "<br>" +
        "Source: " + l.source.name + "<br>" +
        "Target: " + l.target.name + "<br>" +
        "Year: " + l.year + "<br>" +
        "Count: " + l.count + "<br>" +
      "</div>";
  });
graph.call(nodetip);
graph.call(linktip);


// Functions
// ---------------------------------

function displayLoader() {
  $.isLoading({text: "Loading", position: "overlay"});
}
function hideLoader() {
  $.isLoading("hide");
}

function updateGraph() {
  var node_links = [];
  var nodes = {};
  var i;

  // select links and nodes to display
  var node_names = new Set();
  for (i = 0; i < threshold && i < all_nodes.length; ++i) {
    node_names.add(all_nodes[i].name);
  }

  var addLinks = function (l) {
    if (node_names.has(l.target.name) && node_names.has(l.source.name)) {
      if (!nodes[l.source.name]) {
        nodes[l.source.name] = l.source;
      }
      if (!nodes[l.target.name]) {
        nodes[l.target.name] = l.target;
      }
      node_links.push(l);
    }
  };
  for (i = 0; i < threshold && i < all_nodes.length; ++i) {
    var n = all_nodes[i];
    n.index = i;

    if (i === 0) {
      // center first node
      n.x = (width / 2);
      n.y = (height / 2);
      n.fixed = true;
    }

    node_to_links[n.name].forEach(addLinks);
  }

  // links settings
  force.nodes(d3.values(nodes))
    .links(node_links)
    .linkStrength(function(l) { return Math.log(l.count) * 0.2; })
    .charge(function(l) { return Math.log(l.count) * -250; })
    .gravity(function(l) { return Math.log(l.count) * 1; })
    .on("tick", tick)
    .start();

  links = graph.selectAll("line").data(force.links(), JSON.stringify);

  links.enter().append("line")
    .attr("stroke-opacity", function (d) {
      return Math.log(d.count) / Math.log(max_links);
    })
    .attr("class", function (d) { return "link year" + d.year; })
    .on("mouseover", linktip.show)
    .on("mouseout", linktip.hide);
  links.exit().remove();

  // node settings
  node = graph.selectAll("g.node").data(force.nodes(), JSON.stringify);
  node.selectAll("text").remove();
  var node_enter = node.enter().append("g")
    .attr("class", function(d) {
      var dates = new Set();
      var classes = ["node"];
      node_to_links[d.name].forEach(function (l) {
        dates.add(l.year);
      });
      dates.forEach(function(year) { classes.push("year" + year); });
      if (d.fixed) classes.push("fixed");
      return classes.join(' ');
    })
    .on("mouseover", nodetip.show)
    .on("mouseout", nodetip.hide)
    .on("dblclick", dblclick)
    .call(drag);
  node.exit().remove();

  // set node size
  node_enter.append("circle")
    .attr("style", function(d) {
      var random_hue = _.sample(_.range(0, 256));
      return "fill: hsl(" + random_hue + ", 100%, 60%)";
    })
    .attr("r", function(d) {
      return 1 + d.pageRank * Math.log(d.count) * 20;
    });

  // add node name
  node.append("text")
      .attr("x", 12)
      .attr("dy", ".35em")
      .attr("style", function(d) {
        var styles = [];
        var font_size = Math.log(d.count) * 5;
        styles.push("font-size:" + font_size + "px");
        styles.push("opacity:" + Math.log(d.count) * 0.1);
        return styles.join(";");
      })
      .text(function(d) { return d.name; });

  node.exit().remove();
  links.exit().remove();

  updateDate();

  for (i = 0; i < threshold && i < 1000; i++) { force.tick(); }
  force.stop();

  hideLoader();
}

d3.selection.prototype.moveToFront = function () {
  return this.each(function () {
    this.parentNode.appendChild(this);
  });
};

function updateDate(years) {
  // hide all links and nodes
  d3.selectAll(".link")
    .attr("visibility", "hidden");
  d3.selectAll(".node")
    .attr("visibility", "hidden");

  // re-show links and nodes from selected years
  var year_range = _.range(min_year, max_year + 1);
  year_range.forEach(function(year) {
    d3.selectAll(".year" + year)
      .attr("visibility", "visible");
  });

  hideLoader();
}

function tick() {
  links
    .attr("x1", function (d) { return d.source.x; })
    .attr("y1", function (d) { return d.source.y; })
    .attr("x2", function (d) { return d.target.x; })
    .attr("y2", function (d) { return d.target.y; });

  node.attr("transform", function (d) {
    return "translate(" + d.x + "," + d.y + ")";
  });
}

function dragstart(d) {
  d3.select(this).classed("fixed", d.fixed = true);
}

function dblclick(d) {
  d3.select(this).classed("fixed", d.fixed = false);
}


// noUI sliders
// ---------------------------------

function makeDateSlider() {
  var slider = document.getElementById('date-slider');
  if (slider.noUiSlider) slider.noUiSlider.destroy();
  noUiSlider.create(slider, {
    start: [min_year, max_year],
    connect: true,
    step: 1,
    range: {
      'min': [min_year],
      'max': [max_year]
    },
    pips: {
      mode: 'steps',
      density: '2',
    }
  });
  slider.noUiSlider.on('change', function (values, handle) {
    displayLoader();
    min_year = parseInt(values[0]);
    max_year = parseInt(values[1]);
    updateDate();
  });
}

function makeThresholdSlider() {
  var thresholdSlider = document.getElementById('threshold-slider');
  if (thresholdSlider.noUiSlider) thresholdSlider.noUiSlider.destroy();
  noUiSlider.create(thresholdSlider, {
    start: 100,
    step: 10,
    range: {
      'min': [0],
      'max': [num_nodes]
    },
    pips: {
      mode: 'count',
      values: 10,
      density: 2,
    }
  });
  thresholdSlider.noUiSlider.on('change', function () {
    displayLoader();
    threshold = Math.floor(thresholdSlider.noUiSlider.get());
    updateGraph();
  });
}

// create file menu
function makeFileMenu(){
  var fileMenu = document.getElementById("file-menu");

  for (var i=0; i<listOfAvailableDataFiles.length; i++) {
    var fileName = listOfAvailableDataFiles[i];
    fileName = fileName.substring(0, fileName.length - 5);
    var option = document.createElement("option");
    option.innerHTML = fileName;
    fileMenu.appendChild(option);
  }

  var defaultIndex = listOfAvailableDataFiles.indexOf("graph.json");
  if (defaultIndex >= 0) fileMenu.selectedIndex = defaultIndex;

  fileMenu.onchange = function() {
    displayLoader();
    loadDataAndDrawGraph();
  }
}

// get selected file name
function getSelectedFileName() {
  var fileMenu = document.getElementById("file-menu");
  return "/data/" + listOfAvailableDataFiles[fileMenu.selectedIndex];
}

// Load data
// ---------------------------------

// display loader
displayLoader();
makeFileMenu();


function loadDataAndDrawGraph() {
  all_links = [];
  all_nodes = [];
  node_to_links = {};
  node_by_name = {};

  min_year = 0;
  max_year = 0;

  // import and process node data, then get link data
  d3.json(getSelectedFileName(), function (error, data) {
    if (error) throw error;

    // process nodes
    var nodes = data.nodes;
    num_nodes = nodes.length;
    nodes.forEach(function (n) {
      n.name = n.domain;
      n.count = n.inDegree + n.outDegree;
      n.weight = n.count;

      node_to_links[n.name] = [];
      node_by_name[n.name] = n;
    });
    // sort descending for threshold selecting
    all_nodes = _.sortBy(nodes, 'count').reverse();

    // process link data
    all_links = data.links;
    all_links.forEach(function(l) {
      l.source = l.src;
      l.target = l.dst;
      l.year = l.date.substr(0,4);
      if (l.year < min_year || min_year == 0) {
        min_year = parseInt(l.year);
      }
      if (l.year > max_year || max_year == 0) {
        max_year = parseInt(l.year);
      }

      var source_str = l.source;
      var target_str = l.target;

      l.source = node_by_name[source_str];
      l.target = node_by_name[target_str];
      node_to_links[source_str].push(l);

      if (source_str != target_str) {
        var l2 = l;
        l2.source = node_by_name[target_str];
        l2.target = node_by_name[source_str];
        node_to_links[target_str].push(l2);
      }
    });

    if (max_year == min_year) {
      max_year = max_year + 1;
    }

    makeDateSlider();
    makeThresholdSlider();

    updateGraph();
  });
}

loadDataAndDrawGraph();