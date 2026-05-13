const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { chromium } = require('playwright');

const projectDir = __dirname;
const htmlPath = path.join(projectDir, '机构涨停候选股.html');
const pdfPath = path.join(projectDir, '机构涨停候选股.pdf');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(pathToFileURL(htmlPath).href, { waitUntil: 'networkidle' });
  await page.pdf({
    path: pdfPath,
    format: 'A3',
    landscape: true,
    printBackground: true,
    margin: { top: '12mm', right: '12mm', bottom: '12mm', left: '12mm' }
  });
  await browser.close();
})().catch(err => { console.error(err); process.exit(1); });
