import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import MyLayout from "./Layout";
import dtFlow from '../images/dt_flow.jpg';
import {Col, Row, Typography} from 'antd';

const { Title, Paragraph, } = Typography;

const defaultProps = {};
interface HomeProps extends Readonly<typeof defaultProps> {
}
interface HomeState {}

@inject((stores: IStore) => ({
}))
@observer
export class HomeComponent extends React.Component<
  HomeProps,
  HomeState
> {
  public static defaultProps = defaultProps;
  public state: HomeState = {};

  render() {
    return (
      <MyLayout>
        <Typography>
          <Title>
            Welcome to MINT-DT<br/>
            <span style={{color: "purple", fontSize: "0.9em"}}><i> A framework to construct a transformation pipeline based on a given specification</i></span><br/>
          </Title>
          {/* FIXME: <h1>HOME</h1>
          <h1>{this.props.a}</h1>
          <Button type="danger" onClick={this.props.app.setA}>Click Me</Button> */}
          <Row>
            <Col span={16}>
              <Paragraph style={{textAlign: "left", fontSize: "1.0em"}}>
                The idea is that we use <span style={{ color: "blue" }}>smaller components</span> (we refer to them as <b>adapters</b> or building blocks) which we ’concatenate’ to form a transformation flow (a "pipeline"). This modular design allows us to <span style={{ color: "blue" }}>reuse</span> existing modules and <span style={{ color: "blue" }}>wrap ready-scripts</span> to create a <span style={{ color: "blue" }}>language-and-format-independent module</span> and <span style={{ color: "blue" }}>pipeline</span>.<br/>
                There are three types of components (adapters):<br/>
                • <b><u>Reader Adapter</u></b>. Used as an entry point in the pipeline. It reads an input file (data) and a description of it (D-REPR language).<br/>
                • <b><u>Transformation Adapter</u></b>. A class which performs a trans- formation done in a form of an API endpoint (remote or local) or an in-code script or library (i.e. using python or R). It does not materialize the data into an output, it just reproduces the data.<br/>
                • <b><u>Writer Adapter</u></b>. Used as an exit point in the pipeline. It writes an output file based on a description file (D-REPR language).<br/>
                Each adapter is declared using a semantic description of its attributes (i.e. inputs and outputs). The description enables input data validation and compatibility checking between the concatenated adapters and allows an easier construction of the transformation pipeline based on some simple input from the user.<br/>
                The figure on the right depicts the general idea of our architecture that is based on building-blocks and components that can be concatenated. In the figure we show a simplified scheme of a transformation pipeline involving a reader adapter, two transformation adapters (’TA’) and a writer adapter.<br/>
                <i>As an example, a <span style={{ color: "blue" }}>reader adapter</span> may be a CSV-file-reader that encodes the file's content to an intermediate representation. A <span style={{ color: "purple" }}>transformation adapter</span> may perform unit conversions on the data. Other components, such as Wrap and GraphStr2Str, are used to join and reformat the same resource without changing the actual content of the data. A <span style={{ color: "purple" }}>writer adapter</span> will materialize the output data to some other format, i.e. a NetCDF4 file.</i><br/>
              </Paragraph>
            </Col>
            <Col span={4}>
              <img src={dtFlow} alt=""/>
            </Col>
          </Row>
        </Typography>
      </MyLayout>
    );
  }
}

export default HomeComponent;
