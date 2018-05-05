# BadWolf Drivers

[![Build Status](https://travis-ci.org/google/badwolf-drivers.svg?branch=master)](https://travis-ci.org/google/badwolf-drivers) [![Go Report Card](https://goreportcard.com/badge/github.com/google/badwolf-drivers)](https://goreportcard.com/report/github.com/google/badwolf-drivers) [![GoDoc](https://godoc.org/github.com/google/badwolf-drivers?status.svg)](https://godoc.org/github.com/google/badwolf-drivers) 


BadWolf is a temporal graph store loosely modeled after the concepts introduced
by the
[Resource Description Framework (RDF)](https://en.wikipedia.org/wiki/Resource_Description_Framework).
It presents a flexible storage abstraction, efficient query language, and
data-interchange model for representing a directed graph that accommodates the
storage and linking of arbitrary objects without the need for a rigid schema.

[BadWolf](https://github.com/google/badwolf) main repository contains the
implementation, but no persistent drivers. This repository contains a collection
of driver implementations aim to provide, mainly, persistent storage 
alternatives.
