import React from "react";
import {withStyles, WithStyles} from "@material-ui/styles";
import {observer} from "mobx-react";
import {Button, Col, Form, Icon, Input, Row} from "antd";
import {FormComponentProps} from "antd/lib/form";
import _ from "lodash";

const styles = {};
const defaultProps = {};
interface Props
  extends Readonly<typeof defaultProps>,
    WithStyles<typeof styles>,
    FormComponentProps {}
interface State {}

@observer
export class MyFormComponent extends React.Component<Props, State> {
  public static defaultProps = defaultProps;
  public state: State = {};

  render() {
    const {
      getFieldDecorator,
      getFieldError,
      isFieldTouched
    } = this.props.form;

    // Only show error after a field is touched.
    const fieldErrors = _.fromPairs(
      ["username"].map(field => [
        field,
        isFieldTouched(field) && getFieldError(field)
      ])
    );
    const hasError = _.some(fieldErrors);

    return (
      <Row>
        <Col span={12}>
          <Form layout="inline">
            <Form.Item
              validateStatus={fieldErrors.username ? "error" : ""}
              help={fieldErrors.username || ""}
            >
              {getFieldDecorator("username", {
                rules: [
                  { required: true, message: "Please input your username!" }
                ]
              })(
                <Input
                  prefix={
                    <Icon type="user" style={{ color: "rgba(0,0,0,.25)" }} />
                  }
                  placeholder="Username"
                />
              )}
            </Form.Item>
            <Form.Item>
              <Button type="primary" htmlType="submit" disabled={hasError}>
                Log in
              </Button>
            </Form.Item>
          </Form>
        </Col>
      </Row>
    );
  }
}

export default Form.create({})(withStyles(styles)(MyFormComponent));
