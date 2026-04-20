import { Fragment } from "react";
import { jsx, jsxs } from "react/jsx-runtime";

import { toJsxRuntime } from "hast-util-to-jsx-runtime";
import { type BundledLanguage, codeToHast } from "shiki";

export const SHIKI_THEMES = {
  light: "catppuccin-latte",
  dark: "catppuccin-mocha",
} as const;

export async function renderShiki(code: string, lang: BundledLanguage) {
  const hast = await codeToHast(code, { lang, themes: SHIKI_THEMES });
  return toJsxRuntime(hast, { Fragment, jsx, jsxs });
}
