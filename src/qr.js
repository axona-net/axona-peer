// QR code rendering for axona-peer's share section.
//
// One job: take a string (almost always window.location.href) and
// drop an SVG QR code into a target element.  The SVG is generated
// inline from Project Nayuki's qrcodegen library — no canvas, no
// CDN, no network call.  SVG scales crisply at any pixel density,
// which matters because phone cameras decode big-and-sharp better
// than small-and-anti-aliased.
//
// We don't expose options yet.  When real-world use shows we need
// them — different error-correction level, foreground colour, click
// to expand — we'll add them here, not at the call site.

import qrcodegen from './vendor/qrcodegen.js';

const ECC = qrcodegen.QrCode.Ecc.MEDIUM;  // ~15% of modules can be damaged
const BORDER = 2;                          // quiet zone, in modules

/**
 * Render `text` as a QR code SVG inside `targetEl`, replacing any
 * existing content.  Returns the QR code's module count (size in
 * modules per side) for callers that want to log it.
 *
 * The SVG is sized by CSS — it has viewBox and `width="100%"
 * height="100%"`, so the parent's dimensions decide the rendered
 * pixel size.  Don't set a hard pixel size here; that's a layout
 * concern, not a generator concern.
 *
 * @param {Element} targetEl
 * @param {string}  text
 * @returns {number} modules-per-side
 */
export function renderQR(targetEl, text) {
  const qr = qrcodegen.QrCode.encodeText(text, ECC);
  const dim = qr.size + BORDER * 2;

  // Build the SVG as a string and inject via innerHTML.  Slightly
  // ickier than DOM construction but ~10x faster for hundreds of
  // <rect> elements, and the output is identical.
  const parts = [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${dim} ${dim}" `,
    `stroke="none" shape-rendering="crispEdges">`,
    `<rect width="100%" height="100%" fill="#ffffff"/>`,
    `<path d="`,
  ];
  for (let y = 0; y < qr.size; y++) {
    for (let x = 0; x < qr.size; x++) {
      if (qr.getModule(x, y)) {
        parts.push(`M${x + BORDER},${y + BORDER}h1v1h-1z`);
      }
    }
  }
  parts.push(`" fill="#111111"/></svg>`);

  targetEl.innerHTML = parts.join('');
  return qr.size;
}
