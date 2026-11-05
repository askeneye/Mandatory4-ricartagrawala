package main

// NODE

// Node structure that will implement the RAService defined in the proto
// Node should have an ID, a port, a map of peers, and a Lamport clock

// Node also needs to keep track of its state for the Ricart-Agrawala algorithm
// It should have:
// 1. Mutex to protect its state
// 2. A variable to indicate if it's requesting the CS or currently in the CS
// 3. A timestamp for its request
// 4. Some logic to keep track of how many REPLYs it has received and from whom

// Manage Ricart Agrawala logic
