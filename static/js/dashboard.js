// Dashboard functionality
document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard.js loaded successfully');
    
    // If this file is loaded on the dashboard page, initialize dashboard
    if (document.getElementById('dashboard-container')) {
        initializeDashboard();
    }
    
    // If this file is loaded on the simulator page, initialize simulator
    if (document.getElementById('simulator-form')) {
        initializeSimulator();
    }
});

function initializeDashboard() {
    console.log('Initializing dashboard');
    // Dashboard functionality can be added here
}

function initializeSimulator() {
    console.log('Initializing simulator');
    // Additional simulator functionality can be added here
} 