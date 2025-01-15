import { createProxyMiddleware } from "http-proxy-middleware";

module.exports =  function(app) {
  app.use(
    "/api",
    createProxyMiddleware({
      target: "http://0.0.0.0:6543",
      pathRewrite: { "^/api": "" }
    })
  );
};