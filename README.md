# Mandatoy4 ricart agrawala

# Instructions for running the program

## Run nodes
There are set up 4 ports for nodes to run on: The port depends on the node id, so when running the program you will have to choose between 1, 2, 3 or 4 as the id.

This also means that the program is limited to running 4 nodes at once, as there are not enough ports to run more than 4 nodes.

All 4 nodes must be running, as a node only enters the CS after it has recieved a reply from every other node.

Each node has to be run from its own terminal

## Run node(s) from terminal
From the project root, where `<id>` can be 1 through 4:
go run ./cmd/node `<id>`