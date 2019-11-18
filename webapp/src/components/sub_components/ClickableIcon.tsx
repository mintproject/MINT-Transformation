import React from "react";
import { withStyles, WithStyles } from "@material-ui/styles";
import { observer } from "mobx-react";
import { Icon } from "antd";

const styles = {
  root: {}
};
const defaultProps = {
  style: {}
};
interface Props extends Readonly<typeof defaultProps> {
  onClick?: () => void;
  icon: string;
  color: string;
  pressColor: string;
}
interface State {
  status: "noclick" | "clicking";
}

@observer
export class ClickableIcon extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  public static defaultProps = defaultProps;
  public state: State = {
    status: "noclick"
  };

  onMouseDown = () => {
    this.setState({ status: "clicking" });
  };

  onMouseUp = () => {
    this.setState({ status: "noclick" });
  };

  render() {
    return (
      <span
        onMouseDown={this.onMouseDown}
        onMouseUp={this.onMouseUp}
        onClick={this.props.onClick}
        style={{
          color:
            this.state.status === "noclick"
              ? this.props.color
              : this.props.pressColor
        }}
      >
        <Icon type={this.props.icon} theme="filled" style={this.props.style} />
      </span>
    );
  }
}

export default withStyles(styles)(ClickableIcon);
