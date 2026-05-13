const { chromium } = require('/home/zhangzelin/.hermes/hermes-agent/node_modules/playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto('file:///mnt/d/GitProject/GPgetter/机构涨停候选股.html', { waitUntil: 'networkidle' });
  await page.pdf({
    path: '/mnt/d/GitProject/GPgetter/机构涨停候选股.pdf',
    format: 'A3',
    landscape: true,
    printBackground: true,
    margin: { top: '12mm', right: '12mm', bottom: '12mm', left: '12mm' }
  });
  await browser.close();
})().catch(err => { console.error(err); process.exit(1); });
