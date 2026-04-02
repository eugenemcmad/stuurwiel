// Package application is not used as a Go package; use subpackages:
//
//   - publish: publish routing, ports, and use-case service for inbound API traffic
//   - relay: ring edge relay (consume → policy → forward)
//
// Application code depends on domain and defines ports implemented in infrastructure.
package application
