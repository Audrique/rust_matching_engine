The project is a work in progress.

The orderbook in this project builds upon https://github.com/anthdm/rust-trading-engine.

The warp websocket in this project builds upon https://github.com/zupzup/warp-websockets-example.

There should be a "config.json" file located in the root directory with a valid Deribit API client_id and client_secret:
```
{
"client_id": "your_client_id",
"client_secret": "your_client_secret"
}
```
Note that the API key should have the correct privileges.