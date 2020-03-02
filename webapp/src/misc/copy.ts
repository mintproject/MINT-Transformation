import { message } from "antd";

export function copyText(text: string, itemName?: string) {
  const textArea = document.createElement("textarea");
  textArea.style.position = "fixed";
  (textArea.style as any).bottom = 0;
  (textArea.style as any).left = 0;
  textArea.value = text;
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  try {
    var successful = document.execCommand("copy");
    if (successful) {
      message.success(
        itemName ? `Copy \`${itemName}\` success` : "Copy success",
        0.5
      );
    } else {
      message.error("Cannot copy! Unknown reason");
    }
  } catch (err) {
    message.error(`Oops! Cannot copy! Reason ${err}`);
  }

  document.body.removeChild(textArea);
}
