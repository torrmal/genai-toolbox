// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { loadTools } from "./loadTools.js";

/**
 * These functions runs after the browser finishes loading and parsing HTML structure.
 * This ensures that elements can be safely accessed.
 */
document.addEventListener('DOMContentLoaded', () => {
    const toolDisplayArea = document.getElementById('tool-display-area');
    const secondaryPanelContent = document.getElementById('secondary-panel-content');
    const DEFAULT_TOOLSET = ""; // will return all toolsets

    if (!secondaryPanelContent || !toolDisplayArea) {
        console.error('Required DOM elements not found.');
        return;
    }

    loadTools(secondaryPanelContent, toolDisplayArea, DEFAULT_TOOLSET);
});
