// Screenshots of the qsrv dashboards in a live Grafana, light and dark, for
// review (uploaded as a CI artifact). Fails if a panel shows an error.
// node screenshots.js GRAFANA_URL OUTDIR
const { chromium } = require("playwright");
const [base, out] = process.argv.slice(2);

(async () => {
  const browser = await chromium.launch();
  let failed = false;
  for (const theme of ["light", "dark"]) {
    const ctx = await browser.newContext({
      viewport: { width: 1600, height: 1000 },
      httpCredentials: { username: "admin", password: "admin" },
    });
    const page = await ctx.newPage();
    for (const [uid, extra] of [["qsrv-overview", ""], ["qsrv-queue", "&var-queue=ci%2Forders"], ["qsrv-alerts", ""]]) {
      await page.goto(`${base}/d/${uid}?orgId=1&from=now-15m&to=now&kiosk&theme=${theme}${extra}`);
      await page.waitForLoadState("networkidle");
      // Expand collapsed rows and let panels render.
      await page.waitForTimeout(4000);
      const errors = await page.locator('[data-testid="data-testid Panel status error"], [aria-label="Panel status"]').count();
      if (errors > 0) {
        console.log(`${uid} (${theme}): ${errors} panel(s) with errors`);
        failed = true;
      }
      await page.screenshot({ path: `${out}/${uid}-${theme}.png`, fullPage: true });
      console.log(`${uid} (${theme}) captured`);
    }
    await ctx.close();
  }
  await browser.close();
  process.exit(failed ? 1 : 0);
})();
