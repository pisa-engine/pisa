import * as _ from 'lodash';
declare const Template: {
    getRegex: ((token: string) => RegExp) & _.MemoizedFunction;
    render(template: string, tokens?: {}): string;
};
export default Template;
