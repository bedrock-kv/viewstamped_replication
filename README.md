# Bedrock Viewstamped Replication

[![Build Status](https://github.com/bedrock-kv/viewstamped_replication/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/bedrock-kv/viewstamped_replication/actions/workflows/elixir_ci.yaml?branch=main)
[![Coverage Status](https://coveralls.io/repos/github/bedrock-kv/viewstamped_replication/badge.svg?branch=main)](https://coveralls.io/github/bedrock-kv/viewstamped_replication?branch=main)

An implementation of the Viewstamped Replication consensus algorithm in Elixir that doesn't force a lot of opinions. You can bake the protocol into your own genservers, send messages and manage the logs how you like.

## Installation

```elixir
def deps do
  [
    {:bedrock_viewstamped_replication, "~> 0.2.1"}
  ]
end
```
