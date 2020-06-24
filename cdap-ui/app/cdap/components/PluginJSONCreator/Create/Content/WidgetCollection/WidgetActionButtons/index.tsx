/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetActionButtons: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
  };
};

interface IWidgetActionButtonsProps extends WithStyles<typeof styles> {
  onAddWidgetToGroup: () => void;
  onDeleteWidgetFromGroup: () => void;
}

const WidgetActionButtonsView: React.FC<IWidgetActionButtonsProps> = ({
  classes,
  onAddWidgetToGroup,
  onDeleteWidgetFromGroup,
}) => {
  return (
    <div className={classes.widgetActionButtons}>
      <IconButton onClick={onAddWidgetToGroup}>
        <AddIcon fontSize="small" />
      </IconButton>
      <IconButton onClick={onDeleteWidgetFromGroup} color="secondary">
        <DeleteIcon fontSize="small" />
      </IconButton>
    </div>
  );
};

const WidgetActionButtons = withStyles(styles)(WidgetActionButtonsView);
export default WidgetActionButtons;
