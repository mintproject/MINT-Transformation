import {message} from "antd";
import mdFactory from "markdown-it";
import mdMJ from "markdown-it-mathjax";

const md = mdFactory({
  html: true,
  linkify: true,
  typographer: true
}).use(mdMJ()); // use markdown-it-mathjax to prevent markdown engine processes text inside latex equations

/**
 * Parse reference rules found in a markdown string
 *
 * @param text text to be parsed
 */
function parseReferenceRules(text: string): [string, Set<string>] {
  const regex = /\\ref{([^}]+)}/;
  let newText = "";
  let referenceIds = new Set<string>();

  while (true) {
    let match = text.match(regex);
    if (match === null) {
      // add the rest
      newText += text;
      return [newText, referenceIds];
    }

    if (match.index === undefined) {
      message.error(
        `error while parsing markdown reference rules. check console!`
      );
      console.error(
        "error while parsing markdown reference rules. Details:\n* text=",
        text,
        "\n* match=",
        match
      );
      return [newText, referenceIds];
    }

    let refId = match[1];
    // add previous text, and change the reference
    newText += text.substr(0, match.index);
    newText += `<sup><a href="#${refId}">${refId}</a></sup>`;
    // remove processed text
    text = text.substr(match.index + match[0].length);
    referenceIds.add(refId);
  }
}

/**
 * Parse a markdown string and return serialized DOM objects
 * @param text text to be parsed
 */
export default function parseMD(
  text: string
): { html: string; metadata: { references: Set<string> } } {
  // parse \ref{} rules and convert it to links first
  let result = parseReferenceRules(text);

  // run markdown engine
  return {
    html: md.render(result[0]),
    metadata: {
      references: result[1]
    }
  };
}
