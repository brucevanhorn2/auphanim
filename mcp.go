package main

// mcp subcommand: start an MCP (Model Context Protocol) stdio server.
//
// This allows AI coding agents (Claude Code, Copilot, etc.) to query the
// auphanim event store directly from a chat session.
//
// Usage:
//
//	auphanim mcp [--db auphanim.db]
//
// Claude Code configuration (~/.claude/claude.json or project .claude/settings.json):
//
//	{
//	  "mcpServers": {
//	    "auphanim": {
//	      "type": "stdio",
//	      "command": "auphanim",
//	      "args": ["mcp", "--db", "/path/to/auphanim.db"]
//	    }
//	  }
//	}

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"auphanim/internal/mcp"
	"auphanim/internal/store"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start an MCP stdio server to expose the event store to AI agents",
	Long: `Start a Model Context Protocol (MCP) server on stdin/stdout.

This lets AI coding agents (Claude Code, GitHub Copilot, etc.) query the
auphanim event store during a development session.

Available tools:
  get_summary   Per-panel event/error counts
  get_events    List events with optional panel/type/since/limit filters
  get_errors    Shorthand for get_events filtered to ERROR events

Configure in Claude Code (.claude/settings.json):
  {
    "mcpServers": {
      "auphanim": {
        "type": "stdio",
        "command": "auphanim",
        "args": ["mcp", "--db", "/path/to/auphanim.db"]
      }
    }
  }

The --db flag (on the root command) selects which database file to read.
Default: ./auphanim.db`,
	RunE: runMCP,
}

func init() {
	rootCmd.AddCommand(mcpCmd)
}

func runMCP(cmd *cobra.Command, args []string) error {
	st, err := store.Open(dbFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	srv := mcp.New(st, version, os.Stdin, os.Stdout)

	ctx := cmd.Context()
	if ctx == nil {
		ctx = cmd.Root().Context()
	}

	return srv.Run(ctx)
}
