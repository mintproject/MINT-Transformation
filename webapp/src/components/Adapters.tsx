import React from "react";
import MyLayout from "./Layout";
import {inject, observer, PropTypes} from "mobx-react";
import {IStore} from "../store";
import {Card, Row, Col} from 'antd';
import {AdapterType} from "../store/AdapterStore";
import _ from "lodash";
import "./Adapters.css";

const defaultProps = {};
interface AdapterProps extends Readonly<typeof defaultProps> {
  adapters: AdapterType[],
  getAdapters: () => any,
}
interface AdapterState {
}

@inject((stores: IStore) => ({
  adapters: stores.adapterStore.adapters,
  getAdapters: stores.adapterStore.getAdapters,
}))
@observer
export class AdapterComponent extends React.Component<
  AdapterProps,
  AdapterState
> {
  public static defaultProps = defaultProps;
  public state: AdapterState = {
  };

  componentDidMount() {
    this.props.getAdapters();
  }


  render() {
    // this component renders all existing adapters
    // TODO: similar UI between adapters and pipeline?
    // FIXME: not exactly sure how to manage state and onChange and class props
    // const isCardLoading: boolean = this.props.adapters.length === 0;
    const { adapters } = this.props;
    let adapterMatrix: AdapterType[][] = [];
    for (let idx = 0; idx < adapters.length; idx++) {
      if (idx % 3 === 0) {
        adapterMatrix[idx/3] = [adapters[idx]]
      } else {
        adapterMatrix[Math.floor(idx/3)].push(adapters[idx])
      }
    }
    // console.log(adapterMatrix.map((row, rowIdx) => (row.map((ad, idx) => `row-${rowIdx}-col-${idx}`))));
    return (
      <MyLayout>
        {adapterMatrix.map((row, rowIdx) => (<Row
          key={`row-${rowIdx}`}
          style={{ height: "100%", width: "100%" }}
        >
          {row.map((ad, index) => (
          <Col
            key={`col-${index}-row-${rowIdx}`}
            span={8} style={{ height: "100%" }}
          >
            <Card
              title={ad.friendly_name || ad.id}
              bordered={true}
              style={{ margin: "10px 10px" }}
              key={`card-${index}`}
              className="flip-card"
              bodyStyle={{ height: "100%", width: "100%" }}
            >
            <div className="flip-card-inner">
              <p className="flip-card-front">
                {/* • <b><u>Function Name</u></b>: {ad.id}<br/>
                • <b><u>Friendly Name</u></b>: {ad.friendly_name}<br/> */}
                • <b><u>Function Type</u></b>: {ad.func_type}<br/>
                • <b><u>Description</u></b>: {ad.description.split("\n").join(" ")}<br/>
              </p>
              <p className="flip-card-back">
                • <b><u>Inputs</u></b>: {_.isEmpty(ad.inputs) ? <p>None</p> :Object.keys(ad.inputs).map(
                  (inputKey, idx) => (<pre key={`input-${idx}`}>
                    <b><u>{inputKey}</u></b>:<br/>
                      Type: <input value={ad.inputs[inputKey].id} readOnly/>;<br/>
                      Optional: <input value={JSON.stringify(ad.inputs[inputKey].optional)} readOnly/>
                  </pre>))}<br/>
                • <b><u>Outputs</u></b>: {_.isEmpty(ad.outputs) ? <p>None</p> :Object.keys(ad.outputs).map((outputKey, idx) => (
                  <pre key={`input-${idx}`}>
                    <b><u>{outputKey}</u></b>:<br/>
                      Type: <input value={ad.outputs[outputKey].id} readOnly/>;<br/>
                      Optional: <input value={JSON.stringify(ad.outputs[outputKey].optional)} readOnly/>
                  </pre>))}<br/>
              </p>
              </div>
            </Card>
          </Col>))}
        </Row>))}
      </MyLayout>
  ); }
}

export default AdapterComponent;
